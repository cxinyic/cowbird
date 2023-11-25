
#include "cowbird_buffer.h"
#include <chrono>
#include <cstdint>
#include <errno.h>
#include <getopt.h>
#include <iostream>
#include <rdma/rdma_verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <thread>
#include <time.h>
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
#define kRunSeconds 5
#define kRandomIndexes 10000

/* Cowbird related parameters */
#define META_SIZE 3 * sizeof(uint64_t)
#define REQ_BUFFER_SIZE 1073741824
#define RESP_BUFFER_SIZE 1073741824
#define BATCH_SIZE 64

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
  CowbirdBuffer *cowbird_request_buffers;
  CowbirdBuffer *cowbird_response_buffers;
  struct ibv_mr **cowbird_request_mrs;
  struct ibv_mr **cowbird_response_mrs;

  /* remote Cowbird buffers info */
  struct ibv_mr *server_buffer_mr;

  std::vector<std::thread> workers;

  /* other resources for Cowbird */
  struct meta_data **tmp_meta_data;
  uint64_t *nr_write_response;
  uint64_t *nr_read_response;
  uint64_t *nr_write_request;
  uint64_t *nr_read_request;

  /* hash read related structures */
  uint64_t *throughput;
  uint64_t capacity;
  int data_size;
  uint64_t **index_items;
};

void SetThreadAffinity(size_t core) {
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(core, &mask);
  sched_setaffinity(0, sizeof(mask), &mask);
}

int register_RDMA_buffers(struct context *ctx) {
  int i;
  char *cowbird_request_addr, *cowbird_response_addr;

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
    cowbird_response_addr = ctx->cowbird_response_buffers[i].get_addr();
    ctx->cowbird_request_mrs[i] =
        rdma_reg_read(ctx->recv_conn_id[i], cowbird_request_addr,
                      REQ_BUFFER_SIZE + 3 * sizeof(uint64_t));
    if (!ctx->cowbird_request_mrs[i]) {
      VERB_ERR("rdma_reg_read", -1);
      return -1;
    }
    ctx->cowbird_response_mrs[i] =
        rdma_reg_write(ctx->send_conn_id[i], cowbird_response_addr,
                       RESP_BUFFER_SIZE + 3 * sizeof(uint64_t));
    if (!ctx->cowbird_response_mrs[i]) {
      VERB_ERR("rdma_reg_write", -1);
      return -1;
    }
  }
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

void init_param(struct context *ctx) {
  int ret, op;
  struct rdma_addrinfo hints;

  ctx->throughput = new uint64_t[ctx->nr_workers];
  for (int i = 0; i < ctx->nr_workers; i++) {
    ctx->throughput[i] = 0;
  }

  memset(&hints, 0, sizeof(hints));
  hints.ai_port_space = RDMA_PS_TCP;
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
  ctx->cowbird_response_buffers =
      (CowbirdBuffer *)malloc(sizeof(CowbirdBuffer) * ctx->nr_workers);
  ctx->cowbird_request_mrs =
      (struct ibv_mr **)malloc(sizeof(struct ibv_mr *) * ctx->nr_workers);
  ctx->cowbird_response_mrs =
      (struct ibv_mr **)malloc(sizeof(struct ibv_mr *) * ctx->nr_workers);

  for (int i = 0; i < ctx->nr_workers; i++) {
    ctx->cowbird_request_buffers[i] = CowbirdBuffer(REQ_BUFFER_SIZE);
    ctx->cowbird_request_buffers[i].buffer_ =
        (char *)malloc(REQ_BUFFER_SIZE + 3 * sizeof(uint64_t));
    ctx->cowbird_response_buffers[i] = CowbirdBuffer(RESP_BUFFER_SIZE);
    ctx->cowbird_response_buffers[i].buffer_ =
        (char *)malloc(RESP_BUFFER_SIZE + 3 * sizeof(uint64_t));
    ctx->send_bufs[i] =
        (char *)malloc(sizeof(char) * ctx->send_recv_buffer_size);
    ctx->recv_bufs[i] =
        (char *)malloc(sizeof(char) * ctx->send_recv_buffer_size);
  }

  // allocate and init other application related resources
  ctx->nr_write_response =
      (uint64_t *)malloc(ctx->nr_workers * sizeof(uint64_t));
  ctx->nr_read_response =
      (uint64_t *)malloc(ctx->nr_workers * sizeof(uint64_t));
  ctx->nr_write_request =
      (uint64_t *)malloc(ctx->nr_workers * sizeof(uint64_t));
  ctx->nr_read_request = (uint64_t *)malloc(ctx->nr_workers * sizeof(uint64_t));

  ctx->tmp_meta_data =
      (struct meta_data **)malloc(sizeof(struct meta_data *) * ctx->nr_workers);
  for (uint64_t i = 0; i < ctx->nr_workers; i++) {
    ctx->nr_write_response[i] = 0;
    ctx->nr_read_response[i] = 0;
    ctx->nr_write_request[i] = 0;
    ctx->nr_read_request[i] = 0;
    ctx->tmp_meta_data[i] =
        (struct meta_data *)malloc(sizeof(struct meta_data));
  }

  ctx->index_items = (uint64_t **)malloc(sizeof(uint64_t *) * ctx->nr_workers);
  for (int i = 0; i < ctx->nr_workers; i++) {
    ctx->index_items[i] = (uint64_t *)malloc(sizeof(uint64_t) * kRandomIndexes);
  }
}

int init_connect(struct context *ctx) {
  int ret, i;
  struct ibv_qp_init_attr attr;

  printf("CowbirdClient: before connect to server\n");

  for (i = 0; i < ctx->nr_workers; i++) {
    ret = rdma_create_id(NULL, &ctx->send_conn_id[i], NULL, RDMA_PS_TCP);
    if (ret) {
      VERB_ERR("rdma_create_id", ret);
      return ret;
    }

    ret = rdma_resolve_addr(ctx->send_conn_id[i], NULL, ctx->rai->ai_dst_addr,
                            1000);
    if (ret) {
      VERB_ERR("rdma_resolve_addr", ret);
      return ret;
    }

    ret = rdma_resolve_route(ctx->send_conn_id[i], TIMEOUT_IN_MS);
    if (ret) {
      VERB_ERR("rdma_resolve_route", ret);
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

    memset(&attr, 0, sizeof(attr));
    attr.qp_context = ctx;
    attr.cap.max_send_wr = ctx->max_wr;
    attr.cap.max_recv_wr = ctx->max_wr;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 0;
    attr.recv_cq = ctx->send_cqs[i];
    attr.send_cq = ctx->send_cqs[i];

    ret = rdma_create_ep(&ctx->send_conn_id[i], ctx->rai, NULL, &attr);
    if (ret) {
      VERB_ERR("rdma_create_qp", ret);
      return ret;
    }

    ret = rdma_connect(ctx->send_conn_id[i], NULL);
    if (ret) {
      VERB_ERR("rdma_connect", ret);
      return ret;
    }
  }

  for (i = 0; i < ctx->nr_workers; i++) {
    ret = rdma_create_id(NULL, &ctx->recv_conn_id[i], NULL, RDMA_PS_TCP);
    if (ret) {
      VERB_ERR("rdma_create_id", ret);
      return ret;
    }

    ret = rdma_resolve_addr(ctx->recv_conn_id[i], NULL, ctx->rai->ai_dst_addr,
                            1000);
    if (ret) {
      VERB_ERR("rdma_resolve_addr", ret);
      return ret;
    }

    ret = rdma_resolve_route(ctx->recv_conn_id[i], TIMEOUT_IN_MS);
    if (ret) {
      VERB_ERR("rdma_resolve_route", ret);
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

    memset(&attr, 0, sizeof(attr));
    attr.qp_context = ctx;
    attr.cap.max_send_wr = ctx->max_wr;
    attr.cap.max_recv_wr = ctx->max_wr;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 0;
    attr.recv_cq = ctx->recv_cqs[i];
    attr.send_cq = ctx->recv_cqs[i];

    ret = rdma_create_ep(&ctx->recv_conn_id[i], ctx->rai, NULL, &attr);
    if (ret) {
      VERB_ERR("rdma_create_qp", ret);
      return ret;
    }

    ret = rdma_connect(ctx->recv_conn_id[i], NULL);
    if (ret) {
      VERB_ERR("rdma_connect", ret);
      return ret;
    }
  }

  printf("CowbirdClient: after connect to client\n");

  register_RDMA_buffers(ctx);
  printf("CowbirdClient: after register RDMA buffers\n");

  return 0;
}

int exchange_buffer_info(struct context *ctx) {
  int ret, i;
  struct ibv_wc wc;
  printf("CowbirdClient: start buffer info exchange\n");

  for (i = 0; i < ctx->nr_workers; i++) {
    memcpy(ctx->send_bufs[0] + (i * 2) * sizeof(struct ibv_mr),
           ctx->cowbird_request_mrs[i], sizeof(struct ibv_mr));
    memcpy(ctx->send_bufs[0] + (i * 2 + 1) * sizeof(struct ibv_mr),
           ctx->cowbird_response_mrs[i], sizeof(struct ibv_mr));
  }
  // send
  ret = rdma_post_send(ctx->send_conn_id[0], NULL, ctx->send_bufs[0],
                       sizeof(struct ibv_mr) * ctx->nr_workers * 2,
                       ctx->send_mrs[0], IBV_SEND_SIGNALED);

  ret = await_completion_send_cq(ctx, 0);
  while (ibv_poll_cq(ctx->send_cqs[0], 1, &wc)) {
    break;
  }

  //  recv
  ret = rdma_post_recv(ctx->recv_conn_id[0], NULL, ctx->recv_bufs[0],
                       sizeof(struct ibv_mr), ctx->recv_mrs[0]);

  if (ret) {
    VERB_ERR("rdma_post_recv", ret);
    return -1;
  }
  ret = await_completion_recv_cq(ctx, 0);
  while (ibv_poll_cq(ctx->recv_cqs[0], 1, &wc)) {
    memcpy(ctx->server_buffer_mr, ctx->recv_bufs[0], sizeof(struct ibv_mr));
    printf("CowbirdClient: after recv server buffer mr, remote server addr is "
           "%llx\n",
           (unsigned long long)ctx->server_buffer_mr->addr);
    break;
  }
  printf("CowbirdClient: finish buffer info exchange\n");
  return 0;
}

void readAsync(struct context *ctx, int idx, uint64_t req_addr,
               uint64_t resp_addr, uint32_t length, uint64_t &nr) {
  uint64_t start, end;
  uint64_t updated_request_tail;
  void *read_address;
  int cnt;
  read_address = ctx->server_buffer_mr->addr;
  struct meta_data tmp_meta_data;

  tmp_meta_data.is_write = 0;
  tmp_meta_data.req_addr = req_addr;
  tmp_meta_data.resp_addr = resp_addr;
  tmp_meta_data.length = length;

  while (1) {
    if (ctx->cowbird_request_buffers[idx].insert(&tmp_meta_data,
                                                 sizeof(struct meta_data))) {
      break;
    } else {
      ctx->cowbird_response_buffers[idx].read_request_tail(
          &updated_request_tail);
      ctx->cowbird_request_buffers[idx].set_tail(updated_request_tail);
    }
  }
  nr++;
  if (nr % BATCH_SIZE == 0) {
    ctx->cowbird_request_buffers[idx].write_request_head();
  }
  if (nr % 100000 == 0) {
    ctx->cowbird_response_buffers[idx].read_request_tail(&updated_request_tail);
    ctx->cowbird_request_buffers[idx].set_tail(updated_request_tail);
  }
}

void process_RW_response(struct context *ctx, int idx, uint64_t &nr) {
  bool ret;
  uint64_t response_tail;
  struct meta_data tmp_meta_data;
  char *copy_buffer[1000];
  uint64_t start, end;
  int i = 0;
  uint64_t updated_request_tail;

  while (1) {
    // s1: check if there is a response
    ret = ctx->cowbird_response_buffers[idx].poll(&tmp_meta_data,
                                                  sizeof(struct meta_data));
    if (ret == true) {
      // s2: check if the response is for read or write
      if (ctx->tmp_meta_data[idx]->is_write == 0) {
        //   ctx->nr_read_response[idx] += 1;
        // s3: read the response data, we can directly poll since the server
        // will make sure the data is not across the boundary
        ret = ctx->cowbird_response_buffers[idx].poll_check(ctx->data_size);
        nr++;
      }
      // s4: update the response tail
      if (nr % BATCH_SIZE == 0) {
        response_tail = ctx->cowbird_response_buffers[idx].get_tail();
        ctx->cowbird_request_buffers[idx].write_response_tail(response_tail);
      }

    } else {
      // s0: update the request tail and response head
      ctx->cowbird_response_buffers[idx].read_request_tail(
          &updated_request_tail);
      ctx->cowbird_response_buffers[idx].read_response_head();
      ctx->cowbird_request_buffers[idx].set_tail(updated_request_tail);
      break;
    }
  }
}

void readRandomCowbird(struct context *ctx, int idx) {
  uint64_t req_addr, resp_addr;
  uint64_t cnt = 0;
  void *read_address;

  auto start_time = std::chrono::high_resolution_clock::now();
  auto current_time = std::chrono::high_resolution_clock::now();

  read_address = ctx->server_buffer_mr->addr;
  uint64_t nr_req = 0;
  uint64_t nr_resp = 0;
  while (current_time - start_time < std::chrono::seconds(kRunSeconds)) {
    req_addr =
        (uint64_t)read_address + ctx->index_items[idx][cnt % kRandomIndexes];
    readAsync(ctx, idx, req_addr, 0, ctx->data_size, nr_req);
    cnt += 1;
    if (cnt % BATCH_SIZE == 0) {
      process_RW_response(ctx, idx, nr_resp);
    }
    current_time = std::chrono::high_resolution_clock::now();
  }
  ctx->throughput[idx] += nr_resp;
}

void handle_RW_request(struct context *ctx, int idx) {
  SetThreadAffinity(idx);
  printf("CowbirdClient: thread %d is working\n", idx);
  readRandomCowbird(ctx, idx);
}

void print_usage(char *program_name) {
  printf("Usage: %s -n thread_number [-s data_size] \n", program_name);
  printf("Options:\n");
  printf("  -n  Set the number of worker threads (mandatory)\n");
  printf("  -s  Set the size of processed data (optional), default size: "
         "512B\n");
  printf("  -a  Set the IP address (optional)\n");
  printf("  -h  Show ctx help message\n");
}

void gen_random_index(struct context *ctx, int idx) {
  int nr_items = ctx->capacity / ctx->data_size;
  srand(idx);
  for (int i = 0; i < kRandomIndexes; i++) {
    ctx->index_items[idx][i] = (rand() % nr_items) * ctx->data_size;
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
  printf("CowbirdClient: generating keys...\n");
  for (int i = 0; i < ctx.nr_workers; i++) {
    std::thread t(gen_random_index, &ctx, i);
    ctx.workers.push_back(move(t));
  }
  for (auto &worker : ctx.workers) {
    worker.join();
  }
  ctx.workers.clear();
  printf("CowbirdClient: finished generating\n\n");

  for (int i = 0; i < ctx.nr_workers; i++) {
    std::thread t1(handle_RW_request, &ctx, i);
    ctx.workers.push_back(move(t1));
  }
  for (auto &worker : ctx.workers) {
    worker.join();
  }
  printf("\nHash read result: ---------------\n");
  printf("Cowbird, data size is %d\n", ctx.data_size);
  printf("overall time is %d sec\n", kRunSeconds);
  uint64_t sum = 0;
  for (int i = 0; i < ctx.nr_workers; i++) {
    printf("thread %d finished %lu ops\n", i, ctx.throughput[i]);
    sum += ctx.throughput[i];
  }
  printf("finished %lu ops, ", sum);
  printf("throughtput is (%f) MOps\n", sum / (kRunSeconds * 1000000.0));
  printf("---------------------------------\n");

  return ret;
}