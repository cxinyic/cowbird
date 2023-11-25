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
#define kDataSize 64

struct LocalBuffer {
  struct ibv_mr *buffer_mr;
  void *buffer;
};

struct ServerBuffer {
  struct LocalBuffer *recv_buffer;
  struct LocalBuffer *send_buffer;
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
  struct rdma_cm_id **conn_id;
  struct ibv_comp_channel **channels;
  struct ibv_cq **cqs;
  struct ServerBuffer *s_sb_pool;

  std::vector<std::thread> workers;

  /* buffer for RDMA baselines*/
  uint8_t *buffer_pool;
  struct ibv_mr *server_buffer_mr;

  /* hash read related structures */
  int task_type; // 0/1/2: not two-sided; 3: two-sided
  uint64_t capacity;
  int data_size;
};

void SetThreadAffinity(size_t core) {
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(core, &mask);
  sched_setaffinity(0, sizeof(mask), &mask);
}

void destroy_resources(struct context *ctx) {
  int i;

  if (ctx->conn_id) {
    for (i = 0; i < ctx->nr_workers; i++) {
      if (ctx->conn_id[i]) {
        if (ctx->conn_id[i]->qp && ctx->conn_id[i]->qp->state == IBV_QPS_RTS) {
          rdma_disconnect(ctx->conn_id[i]);
        }
        rdma_destroy_qp(ctx->conn_id[i]);
        rdma_destroy_id(ctx->conn_id[i]);
      }
    }

    free(ctx->conn_id);
  }

  for (int i = 0; i < ctx->nr_workers; i++) {
    if (ctx->s_sb_pool[i].recv_buffer->buffer_mr)
      rdma_dereg_mr(ctx->s_sb_pool[i].recv_buffer->buffer_mr);
    if (ctx->s_sb_pool[i].send_buffer->buffer_mr)
      rdma_dereg_mr(ctx->s_sb_pool[i].send_buffer->buffer_mr);
  }
}

int await_completion(struct context *ctx, int idx) {
  int ret;
  struct ibv_cq *ev_cq;
  void *ev_ctx;

  /* Wait for a CQ event to arrive on the channel */
  ret = ibv_get_cq_event(ctx->channels[idx], &ev_cq, &ev_ctx);
  if (ret) {
    VERB_ERR("ibv_get_cq_event", ret);
    return ret;
  }
  ibv_ack_cq_events(ev_cq, 1);
  /* Reload the event notification */
  ret = ibv_req_notify_cq(ctx->cqs[idx], 0);
  if (ret) {
    VERB_ERR("ibv_req_notify_cq", ret);
    return ret;
  }
  return 0;
}

int init_connect(struct context *ctx) {
  int ret, i;
  uint64_t send_count = 0;
  uint64_t recv_count = 0;
  struct ibv_wc wc;
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

  printf("RDMAServer: waiting for connection from RDMAClient ...\n");
  for (i = 0; i < ctx->nr_workers; i++) {
    ret = rdma_get_request(ctx->listen_id, &ctx->conn_id[i]);
    if (ret) {
      VERB_ERR("rdma_get_request", ret);
      return ret;
    }

    ctx->channels[i] = ibv_create_comp_channel(ctx->conn_id[i]->verbs);
    if (!ctx->channels[i]) {
      VERB_ERR("ibv_create_comp_channel", -1);
      return -1;
    }

    ctx->cqs[i] = ibv_create_cq(ctx->conn_id[i]->verbs, ctx->max_wr, NULL,
                                ctx->channels[i], 0);
    if (!ctx->cqs[i]) {
      VERB_ERR("ibv_create_cq", -1);
      return -1;
    }

    ret = ibv_req_notify_cq(ctx->cqs[i], 0);
    if (ret) {
      VERB_ERR("ibv_req_notify_cq", ret);
      return ret;
    }

    memset(&qp_attr, 0, sizeof(qp_attr));

    qp_attr.qp_context = ctx;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = ctx->max_wr;
    qp_attr.cap.max_recv_wr = ctx->max_wr;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.cap.max_inline_data = 0;
    qp_attr.recv_cq = ctx->cqs[i];
    qp_attr.send_cq = ctx->cqs[i];

    ret = rdma_create_qp(ctx->conn_id[i], NULL, &qp_attr);
    if (ret) {
      VERB_ERR("rdma_create_qp", ret);
      return ret;
    }

    ret = rdma_accept(ctx->conn_id[i], NULL);
    if (ret) {
      VERB_ERR("rdma_accept", ret);
      return ret;
    }
  }
  printf("RdmaServer: after connect to client\n");

  for (int i = 0; i < ctx->nr_workers; i++) {
    ctx->s_sb_pool[i].recv_buffer->buffer_mr =
        rdma_reg_msgs(ctx->conn_id[i], ctx->s_sb_pool[i].recv_buffer->buffer,
                      ctx->send_recv_buffer_size);
    if (!ctx->s_sb_pool[i].recv_buffer->buffer_mr) {
      VERB_ERR("rdma_reg_msgs", -1);
      return -1;
    }
    ctx->s_sb_pool[i].send_buffer->buffer_mr =
        rdma_reg_msgs(ctx->conn_id[i], ctx->s_sb_pool[i].send_buffer->buffer,
                      ctx->send_recv_buffer_size);
    if (!ctx->s_sb_pool[i].send_buffer->buffer_mr) {
      VERB_ERR("rdma_reg_msgs", -1);
      return -1;
    }
  }

  ctx->server_buffer_mr = ibv_reg_mr(
      ctx->conn_id[0]->pd, ctx->buffer_pool, ctx->capacity * sizeof(uint8_t),
      (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
       IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));
  if (!ctx->server_buffer_mr) {
    VERB_ERR("fail reg mr", -1);
    return -1;
  }
  printf("RdmaServer: after register RDMA buffers\n");

  memcpy(ctx->s_sb_pool[0].send_buffer->buffer, ctx->server_buffer_mr,
         sizeof(struct ibv_mr));

  ret = rdma_post_send(
      ctx->conn_id[0], NULL, ctx->s_sb_pool[0].send_buffer->buffer,
      sizeof(struct ibv_mr), ctx->s_sb_pool[0].send_buffer->buffer_mr,
      IBV_SEND_SIGNALED);
  if (ret) {
    VERB_ERR("rdma_post_send", ret);
    return -1;
  }
  await_completion(ctx, 0);
  while (1) {
    ret = ibv_poll_cq(ctx->cqs[0], 1, &wc);
    if (ret > 0) {
      break;
    }
  }
  printf("RdmaServer: after send server buffer mr, remote server addr is %p\n",
         ctx->server_buffer_mr->addr);

  return 0;
}
void handle_two_sided(struct context *ctx, int idx) {
  int ret;
  struct ibv_wc wc;
  struct ibv_send_wr send_wr, *bad_send_wr = NULL;
  struct ibv_recv_wr recv_wr, *bad_recv_wr = NULL;
  struct ibv_sge send_sge, recv_sge;

  uint64_t item_index;
  memset(&send_wr, 0, sizeof(send_wr));
  memset(&send_sge, 0, sizeof(send_sge));
  memset(&recv_wr, 0, sizeof(recv_wr));
  memset(&recv_sge, 0, sizeof(recv_sge));
  send_wr.wr_id = idx;
  send_wr.opcode = IBV_WR_SEND;
  send_wr.sg_list = &send_sge;
  send_wr.num_sge = 1;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  send_sge.addr = (uintptr_t)ctx->s_sb_pool[idx].send_buffer->buffer;
  send_sge.length = ctx->data_size;
  send_sge.lkey = ctx->s_sb_pool[idx].send_buffer->buffer_mr->lkey;
  recv_wr.wr_id = idx;
  recv_wr.sg_list = &recv_sge;
  recv_wr.num_sge = 1;
  recv_sge.addr = (uintptr_t)ctx->s_sb_pool[idx].recv_buffer->buffer;
  recv_sge.length = sizeof(int);
  recv_sge.lkey = ctx->s_sb_pool[idx].recv_buffer->buffer_mr->lkey;
  int i = 0;
  int cnt = 0;

  while (1) {
    ret = ibv_post_recv(ctx->conn_id[idx]->qp, &recv_wr, &bad_recv_wr);
    if (ret < 0) {
      VERB_ERR("ibv_post_recv", ret);
    }
    while (1) {
      ret = ibv_poll_cq(ctx->cqs[idx], 1, &wc);
      if (ret < 0) {
        VERB_ERR("ibv_poll_cq", ret);
        break;
      } else if (ret > 0) {
        break;
      }
    }

    i += 1;
    memcpy(&item_index, ctx->s_sb_pool[idx].recv_buffer->buffer,
           sizeof(uint64_t));
    memcpy(ctx->s_sb_pool[idx].send_buffer->buffer,
           ctx->buffer_pool + item_index * ctx->data_size,
           sizeof(ctx->data_size));

    ret = ibv_post_send(ctx->conn_id[idx]->qp, &send_wr, &bad_send_wr);
    if (ret < 0) {
      VERB_ERR("ibv_post_send", ret);
    }
    while (1) {
      ret = ibv_poll_cq(ctx->cqs[idx], 1, &wc);
      if (ret < 0) {
        VERB_ERR("ibv_poll_cq", ret);
        break;
      } else if (ret > 0) {
        break;
      }
    }
  }
}

void handle_RW_request(struct context *ctx, int idx) {
  int core = 2 * idx;
  if (core >= 16)
    core += 16;
  SetThreadAffinity(core);
  handle_two_sided(ctx, idx);
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

  /* allocate memory for RDMA related structures */
  ctx->conn_id = (struct rdma_cm_id **)calloc(ctx->nr_workers,
                                              sizeof(struct rdma_cm_id *));
  ctx->channels = (struct ibv_comp_channel **)calloc(
      ctx->nr_workers, sizeof(struct ibv_comp_channel *));
  ctx->cqs = (struct ibv_cq **)calloc(ctx->nr_workers, sizeof(struct ibv_cq *));

  ctx->server_buffer_mr = new struct ibv_mr();

  ctx->s_sb_pool = (struct ServerBuffer *)malloc(sizeof(struct ServerBuffer) *
                                                 ctx->nr_workers);

  for (int i = 0; i < ctx->nr_workers; i++) {
    ctx->s_sb_pool[i].send_buffer =
        (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
    ctx->s_sb_pool[i].recv_buffer =
        (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
    ctx->s_sb_pool[i].send_buffer->buffer = malloc(ctx->send_recv_buffer_size);
    ctx->s_sb_pool[i].recv_buffer->buffer = malloc(ctx->send_recv_buffer_size);
  }

  ctx->buffer_pool = (uint8_t *)std::malloc(sizeof(uint8_t) * ctx->capacity);
  printf("RDMAServer: after malloc buffer pool, addr is %p\n",
         ctx->buffer_pool);
}

void print_usage(char *program_name) {
  printf("Usage: %s -n thread_number [-s data_size] [-t task_type]\n",
         program_name);
  printf("Options:\n");
  printf("  -n  Set the number of worker threads (mandatory)\n");
  printf(
      "  -s  Set the size of processed data (optional), default size: 64B\n");
  printf("  -a  Set the IP address (optional)\n");
  printf("  -t  Set the task type (optional), default value: 0, (0/1/2: not "
         "two-sided; "
         "3: two-sided)\n");
  printf("  -h  Show this help message\n");
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
  if (ctx.task_type < 0 || ctx.task_type > 3) {
    printf("Error: task type %d is not supported\n", ctx.task_type);
    print_usage(argv[0]);
    exit(1);
  }

  init_param(&ctx);
  init_connect(&ctx);
  switch (ctx.task_type) {
  case 0:
    printf("RdmaServer will do nothing for task type 0\n");
    break;
  case 1:
    printf("RdmaServer will do nothing for task type 1\n");
    break;
  case 2:
    printf("RdmaServer will do nothing for task type 2\n");
    break;
  case 3:
    printf("RdmaServer will handle two-sided requests for task type 3\n");
    break;
  default:
    printf("Error: task type %d is not supported\n", ctx.task_type);
    exit(1);
  }

  if (ctx.task_type == 3) {
    for (int i = 0; i < ctx.nr_workers; i++) {
      std::thread t(handle_RW_request, &ctx, i);
      ctx.workers.push_back(move(t));
    }
    for (auto &worker : ctx.workers) {
      worker.join();
    }
  }
  while (1) {
    usleep(1000);
  }

  destroy_resources(&ctx);
  return ret;
}
