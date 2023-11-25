#include "cowbird_buffer.h"
#include <errno.h>
#include <getopt.h>
#include <infiniband/verbs.h>
#include <iostream>
#include <rdma/rdma_verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <thread>
#include <unistd.h>
#include <vector>

#define VERB_ERR(verb, ret)                                                    \
  fprintf(stderr, "%s returned %d errno %d\n", verb, ret, errno)

/* Default parameters values */
#define DEFAULT_PORT "51216"
#define DEFAULT_SEND_RECV_BUFFER_SIZE 1024000
#define DEFAULT_READ_WRITE_BUFFER_SIZE 256 * 1024 * 1024
#define DEFAULT_MAX_WR 8000
#define TIMEOUT_IN_MS 500 /* ms */

#define kCapacity 1 << 28
#define kDataSize 512

/* Cowbird related parameters */
#define META_SIZE 3 * sizeof(uint64_t)
#define REQ_BUFFER_SIZE 1073741824
#define RESP_BUFFER_SIZE 1073741824
#define BATCH_SIZE 64
#define READ_REQ_BATCH_SIZE 24 * BATCH_SIZE
#define BATCH_POLL_SIZE 32

struct meta_data {
  uint32_t is_write; // 0 for read; 1 for write
  uint32_t length;
  uint64_t req_addr;
  uint64_t resp_addr;
};

struct context {
  /* User parameters */
  char server_name[16];
  char server_port[8];
  int send_recv_buffer_size;
  int nr_workers;
  int max_wr;
  struct rdma_addrinfo *rai;

  /* RDMA related structures */
  struct rdma_cm_id *listen_id;
  struct rdma_cm_id **send_conn_id;
  struct rdma_cm_id **recv_conn_id;
  struct ibv_comp_channel **send_channels;
  struct ibv_comp_channel **recv_channels;
  struct ibv_cq **send_cqs;
  struct ibv_cq **recv_cqs;
  char **send_bufs;
  char **recv_bufs;
  struct ibv_mr **send_mrs;
  struct ibv_mr **recv_mrs;

  /* local Cowbird resources */
  struct ibv_mr **cowbird_request_mrs;
  CowbirdBuffer *cowbird_request_buffers;
  LightCowbirdBuffer *cowbird_response_buffers;

  /* remote Cowbird buffers info */
  struct ibv_mr **client_cowbird_request_mrs;
  struct ibv_mr **client_cowbird_response_mrs;

  std::vector<std::thread> read_workers;
  std::vector<std::thread> process_workers;

  /* buffer for RDMA baselines*/
  uint8_t *buffer_pool;
  struct ibv_mr *server_buffer_mr;

  /* other resources for Cowbird */
  struct meta_data **tmp_meta_data;
  uint64_t *nr_process_read;
  uint64_t *nr_process_write;

  /* hash read related structures */
  uint64_t capacity;
  int data_size;
};

void SetThreadAffinity(size_t core) {
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(core, &mask);
  sched_setaffinity(0, sizeof(mask), &mask);
}

int register_RDMA_buffers(struct context *ctx) {
  int i;
  char *cowbird_request_addr;

  // register two-sided send/recv buffers to RDMA
  for (i = 0; i < ctx->nr_workers; i++) {
    ctx->recv_mrs[i] = rdma_reg_msgs(ctx->recv_conn_id[i], ctx->recv_bufs[i],
                                     ctx->send_recv_buffer_size);
    if (!ctx->recv_mrs[i]) {
      VERB_ERR("rdma_reg_msgs", -1);
      return -1;
    }
    ctx->send_mrs[i] = rdma_reg_msgs(ctx->send_conn_id[i], ctx->send_bufs[i],
                                     ctx->send_recv_buffer_size);
    if (!ctx->send_mrs[i]) {
      VERB_ERR("rdma_reg_msgs", -1);
      return -1;
    }
  }

  // register cowbird request/response buffers to RDMA
  for (i = 0; i < ctx->nr_workers; i++) {
    cowbird_request_addr = ctx->cowbird_request_buffers[i].get_addr();

    ctx->cowbird_request_mrs[i] =
        rdma_reg_msgs(ctx->recv_conn_id[i], cowbird_request_addr,
                      REQ_BUFFER_SIZE + META_SIZE);
    if (!ctx->cowbird_request_mrs[i]) {
      VERB_ERR("rdma_reg_read", -1);
      return -1;
    }
  }
  ctx->server_buffer_mr =
      ibv_reg_mr(ctx->send_conn_id[0]->pd, ctx->buffer_pool, ctx->capacity,
                 (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                  IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));
  if (!ctx->server_buffer_mr) {
    VERB_ERR("fail reg mr", -1);
    return -1;
  }
  return 0;
}
void destroy_resources(struct context *ctx) {
  uint64_t i;

  if (ctx->send_conn_id) {
    for (i = 0; i < ctx->nr_workers; i++) {
      if (ctx->send_conn_id[i]) {
        if (ctx->send_conn_id[i]->qp &&
            ctx->send_conn_id[i]->qp->state == IBV_QPS_RTS) {
          rdma_disconnect(ctx->send_conn_id[i]);
        }
        rdma_destroy_qp(ctx->send_conn_id[i]);
        rdma_destroy_id(ctx->send_conn_id[i]);
      }
    }

    free(ctx->send_conn_id);
  }
  if (ctx->recv_conn_id) {
    for (i = 0; i < ctx->nr_workers; i++) {
      if (ctx->recv_conn_id[i]) {
        if (ctx->recv_conn_id[i]->qp &&
            ctx->recv_conn_id[i]->qp->state == IBV_QPS_RTS) {
          rdma_disconnect(ctx->recv_conn_id[i]);
        }
        rdma_destroy_qp(ctx->recv_conn_id[i]);
        rdma_destroy_id(ctx->recv_conn_id[i]);
      }
    }

    free(ctx->recv_conn_id);
  }
}

int init_connect(struct context *ctx) {
  int ret, i;
  struct ibv_qp_init_attr qp_attr;

  ret = rdma_create_id(NULL, &ctx->listen_id, NULL, RDMA_PS_TCP);
  if (ret) {
    VERB_ERR("rdma_create_id", ret);
    return ret;
  }

  ret = rdma_bind_addr(ctx->listen_id, ctx->rai->ai_src_addr);
  if (ret) {
    VERB_ERR("rdma_bind_addr", ret);
    return ret;
  }

  ret = rdma_listen(ctx->listen_id, 4);
  if (ret) {
    VERB_ERR("rdma_listen", ret);
    return ret;
  }
  printf("CowbirdServer: waiting for connection from RDMAClient ...\n");

  for (i = 0; i < ctx->nr_workers; i++) {
    ret = rdma_get_request(ctx->listen_id, &ctx->send_conn_id[i]);
    if (ret) {
      VERB_ERR("rdma_get_request", ret);
      return ret;
    }

    ctx->send_channels[i] =
        ibv_create_comp_channel(ctx->send_conn_id[i]->verbs);
    if (!ctx->send_channels[i]) {
      VERB_ERR("ibv_create_comp_channel", -1);
      return -1;
    }

    ctx->send_cqs[i] = ibv_create_cq(ctx->send_conn_id[i]->verbs, ctx->max_wr,
                                     NULL, ctx->send_channels[i], 0);
    if (!ctx->send_cqs[i]) {
      VERB_ERR("ibv_create_cq", -1);
      return -1;
    }

    ret = ibv_req_notify_cq(ctx->send_cqs[i], 0);
    if (ret) {
      VERB_ERR("ibv_req_notify_cq", ret);
      return ret;
    }
    /* create the queue pair */
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.qp_context = ctx;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = ctx->max_wr;
    qp_attr.cap.max_recv_wr = ctx->max_wr;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.cap.max_inline_data = 0;
    qp_attr.recv_cq = ctx->send_cqs[i];
    qp_attr.send_cq = ctx->send_cqs[i];

    ret = rdma_create_qp(ctx->send_conn_id[i], NULL, &qp_attr);
    if (ret) {
      VERB_ERR("rdma_create_qp", ret);
      return ret;
    }

    ret = rdma_accept(ctx->send_conn_id[i], NULL);
    if (ret) {
      VERB_ERR("rdma_accept", ret);
      return ret;
    }
  }
  for (i = 0; i < ctx->nr_workers; i++) {
    ret = rdma_get_request(ctx->listen_id, &ctx->recv_conn_id[i]);
    if (ret) {
      VERB_ERR("rdma_get_request", ret);
      return ret;
    }

    ctx->recv_channels[i] =
        ibv_create_comp_channel(ctx->recv_conn_id[i]->verbs);
    if (!ctx->recv_channels[i]) {
      VERB_ERR("ibv_create_comp_channel", -1);
      return -1;
    }

    ctx->recv_cqs[i] = ibv_create_cq(ctx->recv_conn_id[i]->verbs, ctx->max_wr,
                                     NULL, ctx->recv_channels[i], 0);
    if (!ctx->recv_cqs[i]) {
      VERB_ERR("ibv_create_cq", -1);
      return -1;
    }

    ret = ibv_req_notify_cq(ctx->recv_cqs[i], 0);
    if (ret) {
      VERB_ERR("ibv_req_notify_cq", ret);
      return ret;
    }
    /* create the queue pair */
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.qp_context = ctx;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = ctx->max_wr;
    qp_attr.cap.max_recv_wr = ctx->max_wr;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.cap.max_inline_data = 0;
    qp_attr.recv_cq = ctx->recv_cqs[i];
    qp_attr.send_cq = ctx->recv_cqs[i];

    ret = rdma_create_qp(ctx->recv_conn_id[i], NULL, &qp_attr);
    if (ret) {
      VERB_ERR("rdma_create_qp", ret);
      return ret;
    }

    ret = rdma_accept(ctx->recv_conn_id[i], NULL);
    if (ret) {
      VERB_ERR("rdma_accept", ret);
      return ret;
    }
  }
  printf("CowbirdServer: after connect to client\n");

  register_RDMA_buffers(ctx);
  printf("CowbirdServer: after register RDMA buffers\n");
  return 0;
}

int await_completion_send_cq(struct context *ctx, int idx) {
  int ret;
  struct ibv_cq *ev_cq;
  void *ev_ctx;

  /* Wait for a CQ event to arrive on the channel */
  ret = ibv_get_cq_event(ctx->send_channels[idx], &ev_cq, &ev_ctx);
  if (ret) {
    VERB_ERR("ibv_get_cq_event", ret);
    return ret;
  }
  ibv_ack_cq_events(ev_cq, 1);
  /* Reload the event notification */
  ret = ibv_req_notify_cq(ctx->send_cqs[idx], 0);
  if (ret) {
    VERB_ERR("ibv_req_notify_cq", ret);
    return ret;
  }

  return 0;
}

int await_completion_recv_cq(struct context *ctx, int idx) {
  int ret;
  struct ibv_cq *ev_cq;
  void *ev_ctx;

  /* Wait for a CQ event to arrive on the channel */
  ret = ibv_get_cq_event(ctx->recv_channels[idx], &ev_cq, &ev_ctx);
  if (ret) {
    VERB_ERR("ibv_get_cq_event", ret);
    return ret;
  }
  ibv_ack_cq_events(ev_cq, 1);
  /* Reload the event notification */
  ret = ibv_req_notify_cq(ctx->recv_cqs[idx], 0);
  if (ret) {
    VERB_ERR("ibv_req_notify_cq", ret);
    return ret;
  }

  return 0;
}

static void receive_mr(struct context *ctx) {
  int i = 0;
  for (i = 0; i < ctx->nr_workers; i++) {
    memcpy(ctx->client_cowbird_request_mrs[i],
           ctx->recv_bufs[0] + (i * 2) * sizeof(struct ibv_mr),
           sizeof(struct ibv_mr));
    memcpy(ctx->client_cowbird_response_mrs[i],
           ctx->recv_bufs[0] + (i * 2 + 1) * sizeof(struct ibv_mr),
           sizeof(struct ibv_mr));
    printf("CowbirdServer: after recv client cowbird buffer info\n");
  }
}

void init_param(struct context *ctx) {
  int ret, op;
  struct rdma_addrinfo hints;
  ctx->max_wr = DEFAULT_MAX_WR;
  ctx->send_recv_buffer_size = DEFAULT_SEND_RECV_BUFFER_SIZE;

  memset(&hints, 0, sizeof(hints));
  hints.ai_port_space = RDMA_PS_TCP;
  hints.ai_flags = RAI_PASSIVE;

  ret = rdma_getaddrinfo(ctx->server_name, ctx->server_port, &hints, &ctx->rai);
  if (ret) {
    VERB_ERR("rdma_getaddrinfo", ret);
    exit(1);
  }

  // allocate RDMA resources
  ctx->send_conn_id = (struct rdma_cm_id **)calloc(ctx->nr_workers,
                                                   sizeof(struct rdma_cm_id *));
  ctx->recv_conn_id = (struct rdma_cm_id **)calloc(ctx->nr_workers,
                                                   sizeof(struct rdma_cm_id *));
  ctx->send_channels = (struct ibv_comp_channel **)calloc(
      ctx->nr_workers, sizeof(struct ibv_comp_channel *));
  ctx->recv_channels = (struct ibv_comp_channel **)calloc(
      ctx->nr_workers, sizeof(struct ibv_comp_channel *));
  ctx->send_cqs =
      (struct ibv_cq **)calloc(ctx->nr_workers, sizeof(struct ibv_cq *));
  ctx->recv_cqs =
      (struct ibv_cq **)calloc(ctx->nr_workers, sizeof(struct ibv_cq *));
  ctx->send_bufs = (char **)calloc(ctx->nr_workers, sizeof(char *));
  ctx->recv_bufs = (char **)calloc(ctx->nr_workers, sizeof(char *));
  ctx->send_mrs =
      (struct ibv_mr **)calloc(ctx->nr_workers, sizeof(struct ibv_mr *));
  ctx->recv_mrs =
      (struct ibv_mr **)calloc(ctx->nr_workers, sizeof(struct ibv_mr *));
  ctx->server_buffer_mr = new struct ibv_mr();

  // allocate cowbird buffers
  ctx->cowbird_request_buffers =
      (CowbirdBuffer *)malloc(sizeof(CowbirdBuffer) * ctx->nr_workers);
  ctx->cowbird_response_buffers = (LightCowbirdBuffer *)malloc(
      sizeof(LightCowbirdBuffer) * ctx->nr_workers);
  ctx->cowbird_request_mrs =
      (struct ibv_mr **)malloc(sizeof(struct ibv_mr *) * ctx->nr_workers);
  ctx->client_cowbird_request_mrs =
      (struct ibv_mr **)malloc(sizeof(struct ibv_mr *) * ctx->nr_workers);
  ctx->client_cowbird_response_mrs =
      (struct ibv_mr **)malloc(sizeof(struct ibv_mr *) * ctx->nr_workers);

  // allocate and init other resources
  ctx->nr_process_read = (uint64_t *)malloc(ctx->nr_workers * sizeof(uint64_t));
  ctx->nr_process_write =
      (uint64_t *)malloc(ctx->nr_workers * sizeof(uint64_t));
  ctx->tmp_meta_data =
      (struct meta_data **)malloc(sizeof(struct meta_data *) * ctx->nr_workers);
  for (int i = 0; i < ctx->nr_workers; i++) {
    ctx->cowbird_request_buffers[i] = CowbirdBuffer(REQ_BUFFER_SIZE);
    ctx->cowbird_request_buffers[i].buffer_ =
        (char *)malloc(REQ_BUFFER_SIZE + META_SIZE);
    ctx->cowbird_response_buffers[i] = LightCowbirdBuffer(RESP_BUFFER_SIZE);
    ctx->nr_process_write[i] = 0;
    ctx->nr_process_read[i] = 0;
    ctx->tmp_meta_data[i] =
        (struct meta_data *)malloc(sizeof(struct meta_data));
    ctx->client_cowbird_request_mrs[i] =
        (struct ibv_mr *)malloc(sizeof(struct ibv_mr));
    ctx->client_cowbird_response_mrs[i] =
        (struct ibv_mr *)malloc(sizeof(struct ibv_mr));
    ctx->send_bufs[i] =
        (char *)malloc(sizeof(char) * ctx->send_recv_buffer_size);
    ctx->recv_bufs[i] =
        (char *)malloc(sizeof(char) * ctx->send_recv_buffer_size);
  }

  ctx->buffer_pool = (uint8_t *)malloc(ctx->capacity);
}

void exchange_buffer_info(struct context *ctx) {
  int ret;
  struct ibv_wc wc;
  printf("CowbirdServer: start buffer info exchange\n");
  ret = rdma_post_recv(ctx->send_conn_id[0], NULL, ctx->recv_bufs[0],
                       sizeof(struct ibv_mr) * ctx->nr_workers * 2,
                       ctx->recv_mrs[0]);

  ret = await_completion_send_cq(ctx, 0);
  while (ibv_poll_cq(ctx->send_cqs[0], 1, &wc)) {
    receive_mr(ctx);
    break;
  }

  memcpy(ctx->send_bufs[0], ctx->server_buffer_mr, sizeof(struct ibv_mr));
  ret = rdma_post_send(ctx->recv_conn_id[0], NULL, ctx->send_bufs[0],
                       sizeof(struct ibv_mr), ctx->send_mrs[0],
                       IBV_SEND_SIGNALED);
  ret = await_completion_recv_cq(ctx, 0);
  while (ibv_poll_cq(ctx->recv_cqs[0], 1, &wc)) {
    break;
  }
  printf("CowbirdServer: after send server buffer mr, addr is %llx\n",
         (unsigned long long)ctx->server_buffer_mr->addr);
  printf("CowbirdServer: finish buffer info exchange\n");
}

void print_usage(char *program_name) {
  printf("Usage: %s -n thread_number [-s data_size] \n", program_name);
  printf("Options:\n");
  printf("  -n  Set the number of worker threads (mandatory)\n");
  printf(
      "  -s  Set the size of processed data (optional), default size: 512B\n");
  printf("  -a  Set the IP address (optional)\n");
  printf("  -h  Show this help message\n");
}

void rdma_read_meta(struct context *ctx, uint64_t idx) {
  uint64_t ret;
  struct ibv_wc wc;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  memset(&wr, 0, sizeof(wr));
  wr.wr_id = idx;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr =
      (uintptr_t)ctx->client_cowbird_request_mrs[idx]->addr;
  wr.wr.rdma.rkey = ctx->client_cowbird_request_mrs[idx]->rkey;

  sge.addr = (uintptr_t)ctx->recv_bufs[idx];
  sge.length = META_SIZE;
  sge.lkey = ctx->recv_mrs[idx]->lkey;

  ret = ibv_post_send(ctx->recv_conn_id[idx]->qp, &wr, &bad_wr);
  while (1) {
    ret = ibv_poll_cq(ctx->recv_cqs[idx], 1, &wc);
    if (ret > 0)
      break;
  }

  // update request head
  memcpy(&(ctx->cowbird_request_buffers[idx].head_), ctx->recv_bufs[idx],
         sizeof(uint64_t));

  // update response tail
  memcpy(&(ctx->cowbird_response_buffers[idx].tail_),
         ctx->recv_bufs[idx] + sizeof(uint64_t), sizeof(uint64_t));

  // update request real boundary
  memcpy(&(ctx->cowbird_request_buffers[idx].real_boundary_),
         ctx->recv_bufs[idx] + 2 * sizeof(uint64_t), sizeof(uint64_t));
}

void rdma_write_meta(struct context *ctx, uint64_t idx) {
  uint64_t ret;
  struct ibv_wc wc;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  memset(&wr, 0, sizeof(wr));

  // update response head
  memcpy(ctx->send_bufs[idx], &(ctx->cowbird_response_buffers[idx].flush_head_),
         sizeof(uint64_t));

  // update request tail
  memcpy(ctx->send_bufs[idx] + sizeof(uint64_t),
         &(ctx->cowbird_request_buffers[idx].tail_), sizeof(uint64_t));

  // update response real boundary
  memcpy(ctx->send_bufs[idx] + 2 * sizeof(uint64_t),
         &(ctx->cowbird_response_buffers[idx].real_boundary_),
         sizeof(uint64_t));

  wr.wr_id = idx;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr =
      (uintptr_t)ctx->client_cowbird_response_mrs[idx]->addr;
  wr.wr.rdma.rkey = ctx->client_cowbird_response_mrs[idx]->rkey;

  sge.addr = (uintptr_t)ctx->send_bufs[idx];
  sge.length = META_SIZE;
  sge.lkey = ctx->send_mrs[idx]->lkey;

  ret = ibv_post_send(ctx->send_conn_id[idx]->qp, &wr, &bad_wr);
  // while (1) {
  //   ret = ibv_poll_cq(ctx->send_cqs[idx], 1, &wc);
  //   if (ret > 0)
  //     break;
  // }
}

void rdma_read(struct context *ctx, uint64_t idx, int length) {
  uint64_t ret;
  struct ibv_wc wc;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  memset(&wr, 0, sizeof(wr));
  wr.wr_id = idx;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr =
      (uintptr_t)ctx->client_cowbird_request_mrs[idx]->addr + META_SIZE +
      ctx->cowbird_request_buffers[idx].tail_;
  wr.wr.rdma.rkey = ctx->client_cowbird_request_mrs[idx]->rkey;

  sge.addr = (uintptr_t)ctx->cowbird_request_buffers[idx].get_addr() +
             ctx->cowbird_request_buffers[idx].tail_ + META_SIZE;
  sge.length = length;
  sge.lkey = ctx->cowbird_request_mrs[idx]->lkey;

  ret = ibv_post_send(ctx->recv_conn_id[idx]->qp, &wr, &bad_wr);
  while (1) {
    ret = ibv_poll_cq(ctx->recv_cqs[idx], 1, &wc);
    if (ret > 0)
      break;
  }
}

void rdma_write(struct context *ctx, uint64_t idx, int length) {
  uint64_t ret;
  struct ibv_wc wc;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  uint64_t flush_head = ctx->cowbird_response_buffers[idx].get_flush_head();

  memset(&wr, 0, sizeof(wr));
  wr.wr_id = idx;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr =
      (uintptr_t)ctx->client_cowbird_response_mrs[idx]->addr + META_SIZE +
      flush_head;
  wr.wr.rdma.rkey = ctx->client_cowbird_response_mrs[idx]->rkey;

  sge.addr = (uintptr_t)ctx->send_bufs[idx] + META_SIZE;
  sge.length = length;
  sge.lkey = ctx->send_mrs[idx]->lkey;

  ret = ibv_post_send(ctx->send_conn_id[idx]->qp, &wr, &bad_wr);
  // while (1) {
  //   ret = ibv_poll_cq(ctx->send_cqs[idx], 1, &wc);
  //   if (ret > 0)
  //     break;
  // }
  // rdma_write_meta(ctx, idx);
}

void read_requests(struct context *ctx, int idx) {
  int core = idx * 2;
  SetThreadAffinity(core);

  uint64_t nr_read = 0;
  int nr_poll = 0;
  int ret;
  struct ibv_wc wc;

  while (1) {
    while (1) {
      // s1: check whether we need to read meta data from client
      ret = ctx->cowbird_request_buffers[idx].poll_check(READ_REQ_BATCH_SIZE);
      if (ret >= 0) {
        break;
      } else {
        // s2: read meta data from client
        rdma_read_meta(ctx, idx);
      }
    }
    // s3: read requests from client
    if (ret == 0) {
      rdma_read(ctx, idx, READ_REQ_BATCH_SIZE);
      ctx->cowbird_request_buffers[idx].poll_after_check(READ_REQ_BATCH_SIZE);
    } else {
      // printf("we cannot batch, we read with size %d\n", ret);
      rdma_read(ctx, idx, ret);
      ctx->cowbird_request_buffers[idx].poll_after_check(ret);
      // ctx->cowbird_request_buffers[idx].print_buffer();
      // ctx->cowbird_response_buffers[idx].print_buffer();
    }
  }
}
void process_requests(struct context *ctx, int idx) {
  int core = idx * 2 + 1;
  SetThreadAffinity(core);

  int current_write_tail = 0;
  bool ret_process_poll;
  int ret_poll;
  int i = 0;
  uint64_t nr_write = 0;
  int nr_poll = 0;
  struct ibv_wc wc;
  uint64_t start, end;
  int ret;
  int flush_size;
  uint64_t response_size;
  uint64_t nr_poll_all = 0;
  struct meta_data tmp_meta_data;
  int nr = 0;

  while (1) {
    while (1) {
      // s1: read request from local request buffer
      ret_process_poll = ctx->cowbird_request_buffers[idx].process_poll(
          &tmp_meta_data, sizeof(struct meta_data));
      if (ret_process_poll == true) {
        break;
      }
    }
    response_size = sizeof(struct meta_data) + tmp_meta_data.length;

    // s2: parse the request and prepare the response
    while (1) {
      ret = ctx->cowbird_response_buffers[idx].insert_check(response_size);
      if (ret == INSERT_SUCCESS) {
        memcpy(ctx->send_bufs[idx] + current_write_tail + META_SIZE,
               &tmp_meta_data, sizeof(struct meta_data));
        current_write_tail += sizeof(struct meta_data);
        memcpy(ctx->send_bufs[idx] + current_write_tail + META_SIZE,
               (void *)tmp_meta_data.req_addr, ctx->data_size);
        current_write_tail += tmp_meta_data.length;
        ctx->cowbird_response_buffers[idx].insert_after_check(response_size);
        break;
      } else if (ret == INSERT_NEED_FLUSH) {
        // printf("need flush, current_write_tail is %d\n", current_write_tail);
        // ctx->cowbird_request_buffers[idx].print_buffer();
        // ctx->cowbird_response_buffers[idx].print_buffer();
        if (current_write_tail != 0) {
          rdma_write(ctx, idx, current_write_tail);
          ctx->cowbird_response_buffers[idx].update_flush_head(0);
          rdma_write_meta(ctx, idx);
          nr_write += 1;
          current_write_tail = 0;
          if (nr_write % BATCH_POLL_SIZE == 0) {
            while (1) {
              ret_poll = ibv_poll_cq(ctx->send_cqs[idx], 1, &wc);
              if (ret_poll > 0) {
                nr_poll += 1;
                nr_poll_all++;
              }
              if (nr_poll == BATCH_POLL_SIZE * 2) {
                nr_poll = 0;
                break;
              }
            }
          }
        }
        continue;
      } else if (ret == INSERT_FAIL) {
        printf("cannot insert response\n");
        continue;
      }
    }
    // s3: write back the response
    if (current_write_tail >= response_size * BATCH_SIZE * 2) {
      rdma_write(ctx, idx, current_write_tail);
      rdma_write_meta(ctx, idx);
      ctx->cowbird_response_buffers[idx].update_flush_head(current_write_tail);
      current_write_tail = 0;
      nr_write += 1;

      if (nr_write % BATCH_POLL_SIZE == 0) {
        while (1) {
          ret_poll = ibv_poll_cq(ctx->send_cqs[idx], 1, &wc);
          if (ret_poll > 0) {
            nr_poll += 1;
            nr_poll_all++;
          }
          if (nr_poll == BATCH_POLL_SIZE * 2) {
            nr_poll = 0;
            break;
          }
        }
      }
    }
  }
}

int main(int argc, char **argv) {
  int ret, op;
  struct context ctx;
  int n_flag = 0;

  memset(&ctx, 0, sizeof(ctx));

  strcpy(ctx.server_port, DEFAULT_PORT);
  ctx.send_recv_buffer_size = DEFAULT_SEND_RECV_BUFFER_SIZE;
  ctx.max_wr = DEFAULT_MAX_WR;
  strcpy(ctx.server_name, "10.10.1.2");
  ctx.capacity = kCapacity;
  ctx.data_size = kDataSize;

  while ((op = getopt(argc, argv, "n:s:t:a:h")) != -1) {
    switch (op) {
    case 'n':
      ctx.nr_workers = atoi(optarg);
      n_flag = 1;
      break;
    case 's':
      ctx.data_size = atoi(optarg);
      break;
    case 'a':
      strncpy(ctx.server_name, optarg, 15);
      ctx.server_name[15] = '\0';
      break;
    case 'h':
      print_usage(argv[0]);
      exit(0);
    default:
      print_usage(argv[0]);
      exit(1);
    }
  }
  if (!n_flag) {
    printf("Error: Number of worker threads (-n) is mandatory.\n");
    print_usage(argv[0]);
    exit(1);
  }

  init_param(&ctx);
  init_connect(&ctx);
  exchange_buffer_info(&ctx);
  // while(1){}
  for (int i = 0; i < ctx.nr_workers; i++) {
    std::thread read_thread(read_requests, &ctx, i);
    ctx.read_workers.push_back(move(read_thread));

    std::thread process_thread(process_requests, &ctx, i);
    ctx.process_workers.push_back(std::move(process_thread));
  }
  for (auto &worker : ctx.read_workers) {
    worker.join();
  }

  for (auto &worker : ctx.process_workers) {
    worker.join();
  }

  destroy_resources(&ctx);
  return ret;
}
