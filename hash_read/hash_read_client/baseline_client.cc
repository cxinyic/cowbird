
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

#define kDataSize 64
#define kCapacity 1 << 28
#define kRunSeconds 5
#define kRandomIndexes 10000

struct LocalBuffer {
  struct ibv_mr *buffer_mr;
  void *buffer;
};

struct ClientBuffer {
  struct LocalBuffer *send_buffer;
  struct LocalBuffer *recv_buffer;
  struct LocalBuffer *request_buffer;
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
  struct ClientBuffer *c_cb_pool;

  /* buffer for RDMA baselines*/
  struct ibv_mr *server_buffer_mr;
  /* buffer for local baseline*/
  char **content;

  std::vector<std::thread> workers;

  /* hash read related structures */
  int task_type; // 0: local; 1: one-sided sync; 2: one-sided async; 3:
                 // two-sided
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

void init_param(struct context *ctx) {
  int ret, op;
  struct rdma_addrinfo hints;
  ctx->max_wr = DEFAULT_MAX_WR;
  ctx->send_recv_buffer_size = DEFAULT_SEND_RECV_BUFFER_SIZE;

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
  /* allocate memory for RDMA related structures */
  ctx->conn_id = (struct rdma_cm_id **)calloc(ctx->nr_workers,
                                              sizeof(struct rdma_cm_id *));
  ctx->channels = (struct ibv_comp_channel **)calloc(
      ctx->nr_workers, sizeof(struct ibv_comp_channel *));
  ctx->cqs = (struct ibv_cq **)calloc(ctx->nr_workers, sizeof(struct ibv_cq *));

  ctx->server_buffer_mr = new struct ibv_mr();

  ctx->c_cb_pool = (struct ClientBuffer *)malloc(sizeof(struct ClientBuffer) *
                                                 ctx->nr_workers);
  ctx->index_items = (uint64_t **)malloc(sizeof(uint64_t *) * ctx->nr_workers);

  for (int i = 0; i < ctx->nr_workers; i++) {
    ctx->c_cb_pool[i].send_buffer =
        (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
    ctx->c_cb_pool[i].recv_buffer =
        (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
    ctx->c_cb_pool[i].request_buffer =
        (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
    ctx->c_cb_pool[i].send_buffer->buffer = malloc(ctx->send_recv_buffer_size);
    ctx->c_cb_pool[i].recv_buffer->buffer = malloc(ctx->send_recv_buffer_size);
    ctx->c_cb_pool[i].request_buffer->buffer =
        malloc(DEFAULT_READ_WRITE_BUFFER_SIZE);

    ctx->index_items[i] = (uint64_t *)malloc(sizeof(uint64_t) * kRandomIndexes);
  }
}

int init_connect(struct context *ctx) {
  int ret, i, ne;
  struct ibv_wc wc;
  struct ibv_qp_init_attr attr;

  printf("RdmaClient: before connect to server\n");
  for (i = 0; i < ctx->nr_workers; i++) {
    ret = rdma_create_id(NULL, &ctx->conn_id[i], NULL, RDMA_PS_TCP);
    if (ret) {
      VERB_ERR("rdma_create_id", ret);
      return ret;
    }

    ret = rdma_resolve_addr(ctx->conn_id[i], NULL, ctx->rai->ai_dst_addr,
                            TIMEOUT_IN_MS);
    if (ret) {
      VERB_ERR("rdma_resolve_addr", ret);
      return ret;
    }

    ret = rdma_resolve_route(ctx->conn_id[i], TIMEOUT_IN_MS);
    if (ret) {
      VERB_ERR("rdma_resolve_route", ret);
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

    memset(&attr, 0, sizeof(attr));
    attr.qp_context = ctx;
    attr.cap.max_send_wr = ctx->max_wr;
    attr.cap.max_recv_wr = ctx->max_wr;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 0;
    attr.recv_cq = ctx->cqs[i];
    attr.send_cq = ctx->cqs[i];

    ret = rdma_create_ep(&ctx->conn_id[i], ctx->rai, NULL, &attr);
    if (ret) {
      VERB_ERR("rdma_create_qp", ret);
      return ret;
    }

    ret = rdma_connect(ctx->conn_id[i], NULL);
    if (ret) {
      VERB_ERR("rdma_connect", ret);
      return ret;
    }
  }
  printf("RdmaClient: after connect to server\n");

  for (int i = 0; i < ctx->nr_workers; i++) {
    ctx->c_cb_pool[i].send_buffer->buffer_mr =
        rdma_reg_msgs(ctx->conn_id[i], ctx->c_cb_pool[i].send_buffer->buffer,
                      ctx->send_recv_buffer_size);
    if (!ctx->c_cb_pool[i].send_buffer->buffer_mr) {
      VERB_ERR("rdma_reg_msgs", -1);
      return -1;
    }
    ctx->c_cb_pool[i].recv_buffer->buffer_mr =
        rdma_reg_msgs(ctx->conn_id[i], ctx->c_cb_pool[i].recv_buffer->buffer,
                      ctx->send_recv_buffer_size);
    if (!ctx->c_cb_pool[i].recv_buffer->buffer_mr) {
      VERB_ERR("rdma_reg_msgs", -1);
      return -1;
    }

    ctx->c_cb_pool[i].request_buffer->buffer_mr = ibv_reg_mr(
        ctx->conn_id[i]->pd, ctx->c_cb_pool[i].request_buffer->buffer,
        DEFAULT_READ_WRITE_BUFFER_SIZE,
        (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
         IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));
    if (!ctx->c_cb_pool[i].request_buffer->buffer_mr) {
      VERB_ERR("fail reg mr", -1);
      return -1;
    }
  }

  printf("RdmaClient: after register RDMA buffers\n");

  ret = rdma_post_recv(
      ctx->conn_id[0], NULL, ctx->c_cb_pool[0].recv_buffer->buffer,
      sizeof(struct ibv_mr), ctx->c_cb_pool[0].recv_buffer->buffer_mr);
  if (ret) {
    VERB_ERR("rdma_post_recv", ret);
    return -1;
  }
  await_completion(ctx, 0);
  while (1) {
    ret = ibv_poll_cq(ctx->cqs[0], 1, &wc);
    if (ret > 0) {
      memcpy(ctx->server_buffer_mr, ctx->c_cb_pool[0].recv_buffer->buffer,
             sizeof(struct ibv_mr));
      break;
    }
  }

  printf(
      "RdmaClient: after recv server buffer mr, remote server addr is %llx\n",
      (unsigned long long)ctx->server_buffer_mr->addr);
}

void gen_random_index(struct context *ctx, int idx) {
  int nr_items = ctx->capacity / ctx->data_size;
  srand(idx);
  for (int i = 0; i < kRandomIndexes; i++) {
    ctx->index_items[idx][i] = (rand() % nr_items) * ctx->data_size;
  }
}

void readRandomTwoSync(struct context *ctx, int idx) {
  int ret;
  struct ibv_wc wc;
  struct ibv_send_wr send_wr, *bad_send_wr = NULL;
  struct ibv_recv_wr recv_wr, *bad_recv_wr = NULL;
  struct ibv_sge send_sge, recv_sge;

  memset(&send_wr, 0, sizeof(send_wr));
  memset(&send_sge, 0, sizeof(send_sge));
  memset(&recv_wr, 0, sizeof(recv_wr));
  memset(&recv_sge, 0, sizeof(recv_sge));

  send_wr.wr_id = 0;
  send_wr.opcode = IBV_WR_SEND;
  send_wr.sg_list = &send_sge;
  send_wr.num_sge = 1;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  send_sge.addr = (uintptr_t)ctx->c_cb_pool[idx].send_buffer->buffer;
  send_sge.length = sizeof(int);
  send_sge.lkey = ctx->c_cb_pool[idx].send_buffer->buffer_mr->lkey;

  recv_wr.wr_id = 0;
  recv_wr.sg_list = &recv_sge;
  recv_wr.num_sge = 1;
  recv_sge.addr = (uintptr_t)ctx->c_cb_pool[idx].recv_buffer->buffer;
  recv_sge.length = ctx->data_size;
  recv_sge.lkey = ctx->c_cb_pool[idx].recv_buffer->buffer_mr->lkey;

  int i = 0;
  int cnt = 0;
  int poll_cnt = 0;

  auto start_time = std::chrono::high_resolution_clock::now();
  auto current_time = std::chrono::high_resolution_clock::now();

  while (current_time - start_time < std::chrono::seconds(kRunSeconds)) {
    int index = ctx->index_items[idx][cnt % kRandomIndexes] / ctx->data_size;
    memcpy(ctx->c_cb_pool[idx].send_buffer->buffer, &index, sizeof(int));
    ret = ibv_post_send(ctx->conn_id[idx]->qp, &send_wr, &bad_send_wr);
    if (ret < 0) {
      VERB_ERR("ibv_post_send", ret);
    }
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
        poll_cnt += ret;
      }
      if (poll_cnt == 2) {
        poll_cnt = 0;
        break;
      }
    }
    cnt += 1;

    current_time = std::chrono::high_resolution_clock::now();
  }
  ctx->throughput[idx] += cnt;
}

void readRandomLocal(struct context *ctx, int idx) {
  auto start_time = std::chrono::high_resolution_clock::now();
  auto current_time = std::chrono::high_resolution_clock::now();

  uint64_t cnt = 0;
  while (current_time - start_time < std::chrono::seconds(kRunSeconds)) {
    memcpy(ctx->c_cb_pool[idx].recv_buffer->buffer,
           ctx->content[idx] + ctx->index_items[idx][cnt % kRandomIndexes],
           sizeof(ctx->data_size));
    current_time = std::chrono::high_resolution_clock::now();
    cnt += 1;
  }
  ctx->throughput[idx] += cnt;
}

void readRandomSync(struct context *ctx, int idx) {
  int ret;
  struct ibv_wc wc;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  void *read_address;
  uint64_t cnt = 0;

  read_address = ctx->server_buffer_mr->addr;
  memset(&wr, 0, sizeof(wr));
  wr.wr_id = 0;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = (uintptr_t)read_address;
  wr.wr.rdma.rkey = ctx->server_buffer_mr->rkey;

  sge.addr = (uintptr_t)ctx->c_cb_pool[idx].request_buffer->buffer;
  sge.length = ctx->data_size;
  sge.lkey = ctx->c_cb_pool[idx].request_buffer->buffer_mr->lkey;

  auto start_time = std::chrono::high_resolution_clock::now();
  auto current_time = std::chrono::high_resolution_clock::now();
  while (current_time - start_time < std::chrono::seconds(kRunSeconds)) {
    wr.wr.rdma.remote_addr =
        (uintptr_t)read_address + ctx->index_items[idx][cnt % kRandomIndexes];
    ret = ibv_post_send(ctx->conn_id[idx]->qp, &wr, &bad_wr);
    // printf("after post send, addr is %llx\n", wr.wr.rdma.remote_addr);
    if (ret < 0) {
      VERB_ERR("ibv_post_send", ret);
    }
    while (1) {
      ret = ibv_poll_cq(ctx->cqs[idx], 1, &wc);
      if (ret > 0) {
        // printf("cnt is %d\n", cnt);
        break;
      }
      if (ret < 0) {
        VERB_ERR("ibv_poll_cq", ret);
        break;
      }
    }
    cnt += 1;
    current_time = std::chrono::high_resolution_clock::now();
  }
  ctx->throughput[idx] += cnt;
}

void readRandomAsync(struct context *ctx, int idx) {
  int ret;
  struct ibv_wc wc;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  void *read_address;
  uint64_t cnt = 0;
  int batch_size = 200;
  int poll_cnt = 0;
  int nr = 0;

  read_address = ctx->server_buffer_mr->addr;
  memset(&wr, 0, sizeof(wr));
  wr.wr_id = 0;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = (uintptr_t)read_address;
  wr.wr.rdma.rkey = ctx->server_buffer_mr->rkey;

  sge.addr = (uintptr_t)ctx->c_cb_pool[idx].request_buffer->buffer;
  sge.length = ctx->data_size;
  sge.lkey = ctx->c_cb_pool[idx].request_buffer->buffer_mr->lkey;
  auto start_time = std::chrono::high_resolution_clock::now();
  auto current_time = std::chrono::high_resolution_clock::now();
  while (current_time - start_time < std::chrono::seconds(kRunSeconds)) {
    sge.addr = (uintptr_t)ctx->c_cb_pool[idx].request_buffer->buffer +
               ctx->data_size * (cnt % batch_size);
    wr.wr.rdma.remote_addr =
        (uintptr_t)read_address + ctx->index_items[idx][cnt % kRandomIndexes];
    ret = ibv_post_send(ctx->conn_id[idx]->qp, &wr, &bad_wr);
    if (ret < 0) {
      VERB_ERR("ibv_post_send", ret);
    }
    cnt += 1;
    if (cnt % batch_size == 0) {
      poll_cnt = 0;
      while (1) {
        ret = ibv_poll_cq(ctx->cqs[idx], 1, &wc);
        if (ret > 0) {
          poll_cnt++;
          nr++;
        }
        if (ret < 0) {
          VERB_ERR("ibv_poll_cq", ret);
          break;
        }
        if (poll_cnt == batch_size) {
          break;
        }
      }
    }
    current_time = std::chrono::high_resolution_clock::now();
  }
  ctx->throughput[idx] += nr;
}

void handle_RW_request(struct context *ctx, int idx) {
  int core = 2 * idx;
  if (core >= 16)
    core += 16;
  SetThreadAffinity(core);
  printf("RDMAClient: thread %d is working\n", idx);
  switch (ctx->task_type) {
  case 0:
    readRandomLocal(ctx, idx);
    break;
  case 1:
    readRandomSync(ctx, idx);
    break;
  case 2:
    readRandomAsync(ctx, idx);
    break;
  case 3:
    readRandomTwoSync(ctx, idx);
    break;
  default:
    printf("Error: task type %d is not supported\n", ctx->task_type);
    break;
  }
}

void print_usage(char *program_name) {
  printf("Usage: %s -n thread_number [-s data_size] [-t task_type]\n",
         program_name);
  printf("Options:\n");
  printf("  -n  Set the number of worker threads (mandatory)\n");
  printf(
      "  -s  Set the size of processed data (optional), default size: 64B\n");
  printf("  -t  Set the task type (optional), default type: local, (0: local; "
         "1: one-sided sync; 2: one-sided async; 3: two-sided)\n");
  printf("  -a  Set the IP address (optional)\n");
  printf("  -h  Show this help message\n");
}

int main(int argc, char **argv) {
  int ret, op;
  struct context ctx;
  int n_flag = 0;
  struct timeval tv_begin, tv_end;

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
    case 't':
      ctx.task_type = atoi(optarg);
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
  }
  if (ctx.task_type < 0 || ctx.task_type > 3) {
    printf("Error: task type %d is not supported\n", ctx.task_type);
    print_usage(argv[0]);
    exit(1);
  }
  init_param(&ctx);
  if (ctx.task_type != 0) {
    init_connect(&ctx);
  }

  ctx.content = (char **)malloc(ctx.nr_workers * sizeof(char *));
  for (int i = 0; i < ctx.nr_workers; i++) {
    ctx.content[i] = (char *)malloc(ctx.capacity);
    memset(ctx.content[i], 1, ctx.capacity);
  }

  printf("RDMAClient: generating keys...\n");
  for (int i = 0; i < ctx.nr_workers; i++) {
    std::thread t(gen_random_index, &ctx, i);
    ctx.workers.push_back(move(t));
  }
  for (auto &worker : ctx.workers) {
    worker.join();
  }
  ctx.workers.clear();
  printf("RDMAClient: finished generating\n\n");
  switch (ctx.task_type) {
  case 0:
    printf("Start executing local baseline\n");
    break;
  case 1:
    printf("Start executing one-sided sync baseline\n");
    break;
  case 2:
    printf("Start executing one-sided async baseline\n");
    break;
  case 3:
    printf("Start executing two-sided baseline\n");
    break;
  default:
    printf("Error: task type %d is not supported\n", ctx.task_type);
    exit(1);
  }

  gettimeofday(&tv_begin, NULL);
  for (int i = 0; i < ctx.nr_workers; i++) {
    std::thread t1(handle_RW_request, &ctx, i);
    ctx.workers.push_back(move(t1));
  }
  for (auto &worker : ctx.workers) {
    worker.join();
  }
  gettimeofday(&tv_end, NULL);
  printf("\nHash read result: ---------------\n");
  switch (ctx.task_type) {
  case 0:
    printf("local baseline, ");
    break;
  case 1:
    printf("one-sided sync, ");
    break;
  case 2:
    printf("one-sided async, ");
    break;
  case 3:
    printf("two-sided, ");
    break;
  }
  printf("data size is %d\n", ctx.data_size);
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
