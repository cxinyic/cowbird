#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <errno.h>
#include <getopt.h>
#include <rdma/rdma_verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>

#include "../core/gc_state.h"
#include "../core/guid.h"
#include "../core/light_epoch.h"
#include "core/thread.h"
/* Default parameters values */
#define DEFAULT_PORT "51218"
#define DEFAULT_MSG_LENGTH 10486080
#define DEFAULT_MAX_WR 4096

#define BATCH_SIZE 4096
#define PAGE_SIZE 4096
#define DATA_SIZE 8192
#define COMPUTE_BUFFER_SIZE 1073741824 + 2 * sizeof(int)
#define PIPELINE_THRESHOLD 4096
#define WRITE_SIZE 4096
#define TIMEOUT_IN_MS 500 /* ms */
#define META_SIZE 2 * sizeof(int)
#define MAX_ADDR 67108864
#define ADDR_WIDE 8
#define READ_BUFFER_SIZE 16384
#define ASYNC

#define VERB_ERR(verb, ret)                                                    \
  fprintf(stderr, "%s returned %d errno %d\n", verb, ret, errno)

namespace FASTER {
namespace core {

struct meta_data {
  int rw_type; // 0 for read; 1 for write
  uint64_t req_addr;
  uint64_t resp_addr;
  uint32_t length;
};

struct read_across {
  struct meta_data *read_meta;
  int tail;
};

struct LocalBuffer {
  struct ibv_mr *buffer_mr;
  void *buffer;
};

struct ClientBuffer {
  struct LocalBuffer *send_buffer;
  struct LocalBuffer *recv_buffer;
};

class rdma_context {
public:
  /* User parameters */
  int server;
  char *server_port;
  int msg_length;
  int nr_workers;
  int max_wr;
  struct rdma_addrinfo *rai;

  /* Resources */
  struct rdma_cm_id *listen_id;
  struct rdma_cm_id **conn_id;
  struct ClientBuffer *c_cb_pool;
  struct ibv_mr *server_buffer_mr;
  struct ibv_comp_channel **channels;
  struct ibv_cq **cqs;

  std::vector<std::thread> workers;

  uint64_t kSegmentSize;
  uint64_t kCapacity;

  int *nr_reads;
  int *time_comm_us;
  uint64_t **resp_addr_lists;
  int *count;

  void init_param(char *server_ip, int num_threads);
  int init_connect();
  int await_completion(int idx);
  void readAsync(int idx, uint64_t req_addr, uint64_t resp_addr,
                 uint32_t length);
  void writeAsync(int idx, uint64_t req_addr, uint64_t resp_addr,
                  uint32_t length, void *src);
  void poll_RW_process(int idx);
};

int rdma_context::await_completion(int idx) {
  int ret;
  struct ibv_cq *ev_cq;
  void *ev_ctx;

  ret = ibv_get_cq_event(this->channels[idx], &ev_cq, &ev_ctx);
  if (ret) {
    VERB_ERR("ibv_get_cq_event", ret);
    return ret;
  }
  ibv_ack_cq_events(ev_cq, 1);
  ret = ibv_req_notify_cq(this->cqs[idx], 0);
  if (ret) {
    VERB_ERR("ibv_req_notify_cq", ret);
    return ret;
  }
  return 0;
}

void rdma_context::readAsync(int idx, uint64_t req_addr, uint64_t resp_addr,
                             uint32_t length) {
  struct timeval tv_begin, tv_end;
  std::chrono::time_point<std::chrono::high_resolution_clock> start, stop;
  int ret;
  struct ibv_wc wc;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  void *read_address;
  uint64_t segment;
  segment = req_addr / this->kSegmentSize;
  read_address = this->server_buffer_mr->addr + segment * (this->kSegmentSize) +
                 (req_addr % this->kSegmentSize);
  memset(&wr, 0, sizeof(wr));
  wr.wr_id = idx;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = (uintptr_t)read_address;
  wr.wr.rdma.rkey = this->server_buffer_mr->rkey;
#ifdef ASYNC
  sge.addr = (uintptr_t)this->c_cb_pool[idx].recv_buffer->buffer +
             this->nr_reads[idx] * length;
#else
  sge.addr = (uintptr_t)this->c_cb_pool[idx].recv_buffer->buffer;
#endif
  sge.length = length;
  sge.lkey = this->c_cb_pool[idx].recv_buffer->buffer_mr->lkey;
  // gettimeofday(&tv_begin, NULL);
  ret = ibv_post_send(this->conn_id[idx]->qp, &wr, &bad_wr);
#ifdef ASYNC
#else
  while (1) {
    ret = ibv_poll_cq(this->cqs[idx], 1, &wc);
    if (ret > 0)
      break;
  }
#endif

  if (!(void *)resp_addr) {
    printf("NULL pointer\n");
  } else {
#ifdef ASYNC
    this->resp_addr_lists[idx][this->nr_reads[idx]] = resp_addr;
#else
    memcpy((void *)resp_addr, this->c_cb_pool[idx].recv_buffer->buffer, length);
#endif
  }
  this->nr_reads[idx] += 1;
  // gettimeofday(&tv_end, NULL);
  this->time_comm_us[idx] += (tv_end.tv_sec - tv_begin.tv_sec) * 1000000 +
                             (tv_end.tv_usec - tv_begin.tv_usec);
  this->count[idx] += 1;
#ifdef ASYNC
  if (this->nr_reads[idx] == 100) {
    poll_RW_response(idx);
  }
#endif
}

void rdma_context::writeAsync(int idx, uint64_t req_addr, uint64_t resp_addr,
                              uint32_t length, void *src) {
  int ret;
  struct ibv_wc wc;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  void *write_address;
  memcpy(this->c_cb_pool[idx].send_buffer->buffer, src, length);

  uint64_t segment = req_addr / this->kSegmentSize;
  write_address = this->server_buffer_mr->addr +
                  segment * (this->kSegmentSize) +
                  (req_addr % this->kSegmentSize);
  memset(&wr, 0, sizeof(wr));
  wr.wr_id = idx;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = (uintptr_t)write_address;
  wr.wr.rdma.rkey = this->server_buffer_mr->rkey;

  sge.addr = (uintptr_t)this->c_cb_pool[idx].send_buffer->buffer;
  sge.length = length;
  sge.lkey = this->c_cb_pool[idx].send_buffer->buffer_mr->lkey;
  ret = ibv_post_send(this->conn_id[idx]->qp, &wr, &bad_wr);
  while (1) {
    ret = ibv_poll_cq(this->cqs[idx], 1, &wc);
    if (ret > 0)
      break;
  }
}

void rdma_context::poll_RW_process(int idx) {
  struct timeval tv_begin, tv_end;
  int ret;
  struct ibv_wc wc;
  int cnt = 0;
  int length = 576;
  if (this->nr_reads[idx] == 0) {
    return;
  }
  // gettimeofday(&tv_begin, NULL);
  while (1) {
    ret = ibv_poll_cq(this->cqs[idx], 1, &wc);
    if (ret > 0) {
      memcpy((void *)this->resp_addr_lists[idx][cnt],
             this->c_cb_pool[idx].recv_buffer->buffer + cnt * length, length);
      cnt += 1;
    }
    if (cnt > this->nr_reads[idx] - 1) {
      break;
    }
    if (ret < 0) {
      break;
    }
  }
  // gettimeofday(&tv_end, NULL);
  // this->time_comm_us[idx] += (tv_end.tv_sec - tv_begin.tv_sec) * 1000000 +
  // (tv_end.tv_usec - tv_begin.tv_usec);
  this->nr_reads[idx] = 0;
}

void rdma_context::init_param(char *server_ip, int num_threads) {
  int ret, op;
  struct rdma_addrinfo hints;
  this->server_port = DEFAULT_PORT;
  this->max_wr = DEFAULT_MAX_WR;
  this->msg_length = DEFAULT_MSG_LENGTH;
  this->nr_workers = num_threads;

  memset(&hints, 0, sizeof(hints));
  hints.ai_port_space = RDMA_PS_TCP;
  ret = rdma_getaddrinfo(server_ip, this->server_port, &hints, &this->rai);
  if (ret) {
    VERB_ERR("rdma_getaddrinfo", ret);
    exit(1);
  }
  this->conn_id = (struct rdma_cm_id **)calloc(this->nr_workers,
                                               sizeof(struct rdma_cm_id *));
  memset(this->conn_id, 0, sizeof(this->conn_id));
  this->channels = (struct ibv_comp_channel **)calloc(
      this->nr_workers, sizeof(struct ibv_comp_channel *));
  memset(this->channels, 0, sizeof(this->channels));
  this->cqs =
      (struct ibv_cq **)calloc(this->nr_workers, sizeof(struct ibv_cq *));
  memset(this->cqs, 0, sizeof(this->cqs));

  this->server_buffer_mr = new struct ibv_mr();

  this->c_cb_pool = (struct ClientBuffer *)malloc(sizeof(struct ClientBuffer) *
                                                  this->nr_workers);
  this->nr_reads = (int *)malloc(sizeof(int) * this->nr_workers);
  this->resp_addr_lists =
      (uint64_t **)malloc(sizeof(uint64_t *) * this->nr_workers);

  this->time_comm_us = (int *)malloc(sizeof(int) * this->nr_workers);
  this->count = (int *)malloc(sizeof(int) * this->nr_workers);

  for (int i = 0; i < this->nr_workers; i++) {
    this->c_cb_pool[i].send_buffer =
        (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
    this->c_cb_pool[i].recv_buffer =
        (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
    this->c_cb_pool[i].send_buffer->buffer = malloc(this->msg_length);
    this->c_cb_pool[i].recv_buffer->buffer = malloc(this->msg_length);
    this->nr_reads[i] = 0;
    this->count[i] = 0;
    this->resp_addr_lists[i] = (uint64_t *)malloc(sizeof(uint64_t) * 3000);
    this->time_comm_us[i] = 0;
  }

  this->kCapacity = 17179869184ull;
  this->kSegmentSize = 1073741824ull;
}

int rdma_context::init_connect() {
  int ret, i, ne;
  struct ibv_wc wc;
  struct ibv_qp_init_attr attr;

  for (i = 0; i < this->nr_workers; i++) {
    ret = rdma_create_id(NULL, &this->conn_id[i], NULL, RDMA_PS_TCP);
    if (ret) {
      VERB_ERR("rdma_create_id", ret);
      return ret;
    }
    ret =
        rdma_resolve_addr(this->conn_id[i], NULL, this->rai->ai_dst_addr, 1000);
    if (ret) {
      VERB_ERR("rdma_resolve_addr", ret);
      return ret;
    }
    ret = rdma_resolve_route(this->conn_id[i], TIMEOUT_IN_MS);
    if (ret) {
      VERB_ERR("rdma_resolve_route", ret);
      return ret;
    }
    this->channels[i] = ibv_create_comp_channel(this->conn_id[i]->verbs);
    if (!this->channels[i]) {
      VERB_ERR("ibv_create_comp_channel", -1);
      return -1;
    }
    this->cqs[i] = ibv_create_cq(this->conn_id[i]->verbs, this->max_wr, NULL,
                                 this->channels[i], 0);
    if (!this->cqs[i]) {
      VERB_ERR("ibv_create_cq", -1);
      return -1;
    }
    ret = ibv_req_notify_cq(this->cqs[i], 0);
    if (ret) {
      VERB_ERR("ibv_req_notify_cq", ret);
      return ret;
    }
    memset(&attr, 0, sizeof(attr));
    attr.qp_context = this;
    attr.cap.max_send_wr = this->max_wr;
    attr.cap.max_recv_wr = this->max_wr;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = 0;
    attr.recv_cq = this->cqs[i];
    attr.send_cq = this->cqs[i];
    ret = rdma_create_ep(&this->conn_id[i], this->rai, NULL, &attr);
    if (ret) {
      VERB_ERR("rdma_create_qp", ret);
      return ret;
    }
    ret = rdma_connect(this->conn_id[i], NULL);
    if (ret) {
      VERB_ERR("rdma_connect", ret);
      return ret;
    }
  }
  for (int i = 0; i < this->nr_workers; i++) {
    this->c_cb_pool[i].send_buffer->buffer_mr =
        rdma_reg_msgs(this->conn_id[i], this->c_cb_pool[i].send_buffer->buffer,
                      this->msg_length);
    if (!this->c_cb_pool[i].send_buffer->buffer_mr) {
      VERB_ERR("rdma_reg_msgs", -1);
      return -1;
    }
    this->c_cb_pool[i].recv_buffer->buffer_mr =
        rdma_reg_msgs(this->conn_id[i], this->c_cb_pool[i].recv_buffer->buffer,
                      this->msg_length);
    if (!this->c_cb_pool[i].recv_buffer->buffer_mr) {
      VERB_ERR("rdma_reg_msgs", -1);
      return -1;
    }
  }

  for (int i = 0; i < 1; i++) {
    ret = rdma_post_recv(
        this->conn_id[i], NULL, this->c_cb_pool[i].recv_buffer->buffer,
        this->msg_length, this->c_cb_pool[i].recv_buffer->buffer_mr);

    if (ret) {
      VERB_ERR("rdma_post_recv", ret);
      return -1;
    }
  }
  ret = await_completion(0);
  while (ibv_poll_cq(this->cqs[0], 1, &wc)) {
    memcpy(this->server_buffer_mr, this->c_cb_pool[0].recv_buffer->buffer,
           sizeof(struct ibv_mr));
    break;
  }
  return 0;
}

} // namespace core
} // namespace FASTER
