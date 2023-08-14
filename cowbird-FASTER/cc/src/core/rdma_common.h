#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <errno.h>
#include <getopt.h>
#include <queue>
#include <rdma/rdma_verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/time.h>
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
#define DEFAULT_MSG_LENGTH 10000
#define DEFAULT_MAX_WR 256

#define COMPUTE_BUFFER_SIZE_DATA 1073741824ull
#define COMPUTE_BUFFER_SIZE_METADATA 1073741824ull + 2 * sizeof(uint64_t)
#define COMPUTE_RESPONSE_BUFFER_SIZE_METADATA 4 * sizeof(uint64_t)
#define COMPUTE_BUFFER_SIZE_METADATA_PURE 1073741824ull
#define FAKE_BUFFER_SIZE_PURE 4294967296ull
#define META_SIZE 2 * sizeof(uint64_t)

#define VERB_ERR(verb, ret)                                                    \
  fprintf(stderr, "%s returned %llu errno %llu\n", verb, ret, errno)

uint64_t htonll(uint64_t value) {
  return ((uint64_t)htonl(value & 0xFFFFFFFF) << 32LL) | htonl(value >> 32);
}

uint64_t ntohll(uint64_t value) {
  return ((uint64_t)ntohl(value & 0xFFFFFFFF) << 32LL) | ntohl(value >> 32);
}
struct user_space_map {
  uint32_t offset;
  uint64_t rdma_pool_addr;
  uint64_t user_space_addr;
  uint32_t length;
};
std::vector<std::deque<struct user_space_map>> user_space_queues;
std::deque<struct user_space_map> user_space_queue_1;
std::deque<struct user_space_map> user_space_queue_2;

namespace FASTER {
namespace core {

struct meta_data {
  uint16_t rw_type; // 0 for read; 1 for write
  uint16_t thread_id;
  uint32_t length;
  uint64_t req_addr;
  uint64_t resp_addr;
  uint64_t padding;
};
/* head is between 0~2^32; it is not the real head within the range of
BUFFER_SIZE */
/* real head = head &(buffer_size -1)*/
struct LocalBuffer {
  struct ibv_mr *buffer_mr;
  void *data;
  uint64_t size;
  uint64_t head;
  uint64_t real_head;
  uint64_t tail;
  uint64_t real_tail;
  bool same_page;
};

struct ClientBuffer {
  struct LocalBuffer *metadata_request_buffer;
  struct LocalBuffer *data_request_buffer;
  struct LocalBuffer *metadata_response_buffer;
  struct LocalBuffer *data_response_buffer;
};

class rdma_context {
public:
  /* User parameters */
  uint64_t server;
  char *server_port;
  uint64_t msg_length;
  uint64_t nr_workers;
  uint64_t max_wr;
  struct rdma_addrinfo *rai;

  /* Resources */
  struct rdma_cm_id *srq_id;
  struct rdma_cm_id *listen_id;
  struct rdma_cm_id **conn_id;
  struct ibv_mr *send_mr;
  struct ibv_mr *recv_mr;
  struct ibv_mr *remote_buffer_mr;
  uint64_t remote_buffer_size;
  struct ibv_srq *srq;
  struct ibv_cq *srq_cq;
  struct ibv_comp_channel *srq_cq_channel;
  char *send_buf;
  char *recv_buf;
  struct ClientBuffer *c_cb_pool;
  std::vector<std::thread> workers;
  uint64_t *nr_write_response;
  uint64_t *nr_read_response;
  uint64_t *nr_write_request;
  uint64_t *nr_read_request;
  struct user_space_map tmp_user_space_map_1;
  struct user_space_map tmp_user_space_map_2;
  struct user_space_map *tmp_user_space_maps;

  struct timeval tv_begin, tv_end;
  int *time_comm_us;

  struct meta_data **tmp_meta_data;

  void init_param(char *server_ip, uint64_t num_threads);
  uint64_t init_resources();
  uint64_t init_connect();
  uint64_t send_buffer_mr();

  void local_read_meta(uint64_t idx);
  void local_write_meta(uint64_t idx);

  bool pre_local_write_meta(uint64_t idx, uint64_t length);
  bool pre_local_write_data_response(uint64_t idx, uint64_t length);
  bool pre_local_write_data_request(uint64_t idx, uint64_t length);
  void readAsync(uint64_t idx, uint64_t req_addr, uint64_t resp_addr,
                 uint32_t length);
  void writeAsync(uint64_t idx, uint64_t req_addr, uint64_t resp_addr,
                  uint32_t length, void *src);
  void poll_RW_response(uint64_t idx);
};
/* read the current request tail and response head,
check whether the remote side finishes processing and we can add new requests */
void rdma_context::local_read_meta(uint64_t idx) {
  uint64_t tmp;
  uint64_t tmp1;
  uint64_t decode_tmp;
  memcpy(&tmp, this->c_cb_pool[idx].metadata_response_buffer->data,
         sizeof(uint64_t));
  memcpy(&tmp1,
         this->c_cb_pool[idx].metadata_response_buffer->data + sizeof(uint64_t),
         sizeof(uint64_t));
  /* local read request tail */
  decode_tmp = ntohll(tmp);
  this->c_cb_pool[idx].metadata_request_buffer->tail = decode_tmp;

  /* local read response head */
  decode_tmp = ntohll(tmp1);
  this->c_cb_pool[idx].metadata_response_buffer->head = decode_tmp;
}

/* update the current request head and response tail to the request buffer,
remote side can check and get the newest value */
void rdma_context::local_write_meta(uint64_t idx) {
  /* local write request head */
  uint32_t tmp;
  tmp = htonl(uint32_t(this->c_cb_pool[idx].metadata_request_buffer->head));
  memcpy(this->c_cb_pool[idx].metadata_request_buffer->data, &tmp,
         sizeof(uint32_t));
  tmp = htonl(uint32_t(idx + 1));
  memcpy(this->c_cb_pool[idx].metadata_request_buffer->data + sizeof(uint32_t),
         &tmp, sizeof(uint32_t));
}

/* check whether we can do local write(add new requests in the request pool) */
bool rdma_context::pre_local_write_meta(uint64_t idx, uint64_t length) {
  local_read_meta(idx);
  uint64_t tail_page = this->c_cb_pool[idx].metadata_request_buffer->tail /
                       COMPUTE_BUFFER_SIZE_METADATA_PURE;
  uint64_t head_page =
      (this->c_cb_pool[idx].metadata_request_buffer->head + length) /
      COMPUTE_BUFFER_SIZE_METADATA_PURE;

  uint64_t real_tail = this->c_cb_pool[idx].metadata_request_buffer->tail &
                       (COMPUTE_BUFFER_SIZE_METADATA_PURE - 1);
  uint64_t real_head =
      (this->c_cb_pool[idx].metadata_request_buffer->head + length) &
      (COMPUTE_BUFFER_SIZE_METADATA_PURE - 1);

  if (tail_page == head_page) {
    if (real_tail < real_head) {
      return true;
    } else {
      return false;
    }
  } else {
    if (real_tail < real_head) {
      return false;
    } else {
      return true;
    }
  }
}

bool rdma_context::pre_local_write_data_response(uint64_t idx,
                                                 uint64_t length) {
  if (this->c_cb_pool[idx].data_response_buffer->head + length >
          this->c_cb_pool[idx].data_response_buffer->tail ||
      this->c_cb_pool[idx].data_response_buffer->head <
          this->c_cb_pool[idx].data_response_buffer->tail) {
    return true;
  } else {
    return false;
  }
}

bool rdma_context::pre_local_write_data_request(uint64_t idx, uint64_t length) {
  if (this->c_cb_pool[idx].data_request_buffer->same_page == true) {
    if (this->c_cb_pool[idx].data_request_buffer->head + length >
        this->c_cb_pool[idx].data_request_buffer->tail) {
      return true;
    } else {
      return false;
    }
  }
  if (this->c_cb_pool[idx].data_request_buffer->same_page == false) {
    if (this->c_cb_pool[idx].data_request_buffer->head + length >
        this->c_cb_pool[idx].data_request_buffer->tail) {
      return false;
    } else {
      return true;
    }
  }
}

void rdma_context::readAsync(uint64_t idx, uint64_t req_addr,
                             uint64_t resp_addr, uint32_t length) {
  struct timeval tv_begin, tv_end;
  while (!pre_local_write_meta(idx, sizeof(struct meta_data))) {
  }
  while (!pre_local_write_data_response(idx, length)) {
  }

  /* we calculate the remote addr based on the remote_buffer_addr as the
  base */
  gettimeofday(&tv_begin, NULL);
  req_addr =
      (uintptr_t)remote_buffer_mr->addr + req_addr % this->remote_buffer_size;
  /* we calculate the expected resp addr based on the expected head for
   * data_request_data */
  uint64_t rdma_pool_addr =
      (uintptr_t)(this->c_cb_pool[idx].data_response_buffer->data) +
      this->c_cb_pool[idx].data_response_buffer->head;
  this->tmp_meta_data[idx]->rw_type = 0;
  this->tmp_meta_data[idx]->thread_id = htons(idx + 1);
  this->tmp_meta_data[idx]->req_addr = htonll((uint64_t)req_addr);
  this->tmp_meta_data[idx]->resp_addr = htonll((uint64_t)rdma_pool_addr);
  this->tmp_meta_data[idx]->length = htonl(length);
  this->tmp_meta_data[idx]->padding = 0;

  this->c_cb_pool[idx].metadata_request_buffer->real_head =
      this->c_cb_pool[idx].metadata_request_buffer->head &
      (COMPUTE_BUFFER_SIZE_METADATA_PURE - 1);
  memcpy(this->c_cb_pool[idx].metadata_request_buffer->data +
             this->c_cb_pool[idx].metadata_request_buffer->real_head + 16,
         this->tmp_meta_data[idx], sizeof(struct meta_data));

  /* update both the head of metadata and the expected head of data */
  this->c_cb_pool[idx].metadata_request_buffer->head +=
      sizeof(struct meta_data);
  this->c_cb_pool[idx].data_response_buffer->head += length;

  if (this->c_cb_pool[idx].metadata_request_buffer->head >=
      FAKE_BUFFER_SIZE_PURE) {
    this->c_cb_pool[idx].metadata_request_buffer->head = 0;
  }
  if (this->c_cb_pool[idx].data_response_buffer->head >=
      COMPUTE_BUFFER_SIZE_DATA) {
    this->c_cb_pool[idx].data_response_buffer->head -= COMPUTE_BUFFER_SIZE_DATA;
    this->c_cb_pool[idx].data_response_buffer->same_page = false;
  }

  /* update request head */
  local_write_meta(idx);

  this->tmp_user_space_maps[idx].offset =
      this->c_cb_pool[idx].metadata_request_buffer->head;
  this->tmp_user_space_maps[idx].rdma_pool_addr = rdma_pool_addr;
  this->tmp_user_space_maps[idx].user_space_addr = resp_addr;
  this->tmp_user_space_maps[idx].length = length;
  user_space_queues[idx].push_back(this->tmp_user_space_maps[idx]);
  gettimeofday(&tv_end, NULL);
  this->time_comm_us[idx] += (tv_end.tv_sec - tv_begin.tv_sec) * 1000000 +
                             (tv_end.tv_usec - tv_begin.tv_usec);

  this->nr_read_request[idx] += 1;
}

void rdma_context::writeAsync(uint64_t idx, uint64_t req_addr,
                              uint64_t resp_addr, uint32_t length, void *src) {

  while (!pre_local_write_meta(idx, sizeof(struct meta_data))) {
  }
  while (!pre_local_write_data_request(idx, length)) {
  }

  uint64_t rdma_pool_addr =
      (uintptr_t)(this->c_cb_pool[idx].data_request_buffer->data) +
      this->c_cb_pool[idx].data_request_buffer->head;
  memcpy((void *)rdma_pool_addr, (void *)req_addr, length);

  resp_addr =
      (uintptr_t)remote_buffer_mr->addr + resp_addr % this->remote_buffer_size;

  this->tmp_meta_data[idx]->rw_type = 1;
  this->tmp_meta_data[idx]->thread_id = htons((uint16_t)idx);
  this->tmp_meta_data[idx]->req_addr = htonll((uint64_t)rdma_pool_addr);
  this->tmp_meta_data[idx]->resp_addr = htonll((uint64_t)resp_addr);
  this->tmp_meta_data[idx]->length = htonl(length);
  this->tmp_meta_data[idx]->padding = 0;
  this->c_cb_pool[idx].metadata_request_buffer->real_head =
      this->c_cb_pool[idx].metadata_request_buffer->head &
      (COMPUTE_BUFFER_SIZE_METADATA_PURE - 1);
  memcpy(this->c_cb_pool[idx].metadata_request_buffer->data +
             this->c_cb_pool[idx].metadata_request_buffer->real_head + 16,
         this->tmp_meta_data[idx], sizeof(struct meta_data));

  /* update both the head of metadata and the expected head of data */
  this->c_cb_pool[idx].metadata_request_buffer->head +=
      sizeof(struct meta_data);
  this->c_cb_pool[idx].data_request_buffer->head += length;

  if (this->c_cb_pool[idx].metadata_request_buffer->head >=
      FAKE_BUFFER_SIZE_PURE) {
    this->c_cb_pool[idx].metadata_request_buffer->head = 0;
  }
  if (this->c_cb_pool[idx].data_request_buffer->head >=
      COMPUTE_BUFFER_SIZE_DATA) {
    this->c_cb_pool[idx].data_request_buffer->head -= COMPUTE_BUFFER_SIZE_DATA;
    this->c_cb_pool[idx].data_request_buffer->same_page = false;
  }

  /* update request head */
  local_write_meta(idx);
  /* update number of write request */
  this->nr_write_request[idx] += 1;
}

void rdma_context::poll_RW_response(uint64_t idx) {

  int size = 0;
  local_read_meta(idx);
  while (!user_space_queues[idx].empty()) {
    tmp_user_space_maps[idx] = user_space_queues[idx].front();
    if (tmp_user_space_maps[idx].offset <=
        this->c_cb_pool[idx].metadata_response_buffer->head) {
      user_space_queues[idx].pop_front();
      memcpy((void *)tmp_user_space_maps[idx].user_space_addr,
             (void *)tmp_user_space_maps[idx].rdma_pool_addr,
             tmp_user_space_maps[idx].length);
      this->c_cb_pool[idx].data_response_buffer->tail =
          tmp_user_space_maps[idx].rdma_pool_addr;
    } else {
      break;
    }
  }
}

void rdma_context::init_param(char *server_ip, uint64_t num_threads) {
  uint64_t ret, op;
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
  /* allocate memory for our QPs and send/recv buffers */
  /* one more rdma_cm_id for p4 switch */
  this->conn_id = (struct rdma_cm_id **)calloc(this->nr_workers + 4,
                                               sizeof(struct rdma_cm_id *));
  memset(this->conn_id, 0, sizeof(this->conn_id));
  this->send_buf = (char *)malloc(this->msg_length);
  memset(this->send_buf, 0, this->msg_length);
  this->recv_buf = (char *)malloc(this->msg_length);
  memset(this->recv_buf, 0, this->msg_length);

  this->c_cb_pool = (struct ClientBuffer *)malloc(sizeof(struct ClientBuffer) *
                                                  this->nr_workers);
  this->nr_write_response =
      (uint64_t *)malloc(this->nr_workers * sizeof(uint64_t));
  this->nr_read_response =
      (uint64_t *)malloc(this->nr_workers * sizeof(uint64_t));
  this->nr_write_request =
      (uint64_t *)malloc(this->nr_workers * sizeof(uint64_t));
  this->nr_read_request =
      (uint64_t *)malloc(this->nr_workers * sizeof(uint64_t));
  this->tmp_meta_data = (struct meta_data **)malloc(sizeof(struct meta_data *) *
                                                    this->nr_workers);
  this->tmp_user_space_maps = (struct user_space_map *)malloc(
      this->nr_workers * sizeof(struct user_space_map));
  this->time_comm_us = (int *)malloc(this->nr_workers * sizeof(int));

  for (uint64_t i = 0; i < this->nr_workers; i++) {
    this->c_cb_pool[i].metadata_request_buffer =
        (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
    this->c_cb_pool[i].data_request_buffer =
        (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
    this->c_cb_pool[i].metadata_response_buffer =
        (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
    this->c_cb_pool[i].data_response_buffer =
        (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
    this->c_cb_pool[i].metadata_request_buffer->data =
        malloc(COMPUTE_BUFFER_SIZE_METADATA);
    memset(this->c_cb_pool[i].metadata_request_buffer->data, 0,
           COMPUTE_BUFFER_SIZE_METADATA);
    this->c_cb_pool[i].data_request_buffer->data =
        malloc(COMPUTE_BUFFER_SIZE_DATA);
    this->c_cb_pool[i].metadata_response_buffer->data = malloc(16);
    memset(this->c_cb_pool[i].metadata_response_buffer->data, 0, 16);
    this->c_cb_pool[i].data_response_buffer->data =
        malloc(COMPUTE_BUFFER_SIZE_DATA);

    this->c_cb_pool[i].metadata_request_buffer->head = 0;
    this->c_cb_pool[i].metadata_request_buffer->tail = 0;
    this->c_cb_pool[i].metadata_request_buffer->same_page = true;
    this->c_cb_pool[i].metadata_response_buffer->head = 0;
    this->c_cb_pool[i].metadata_response_buffer->tail = 0;
    this->c_cb_pool[i].metadata_response_buffer->same_page = true;
    this->c_cb_pool[i].data_request_buffer->head = 0;
    this->c_cb_pool[i].data_request_buffer->tail = 0;
    this->c_cb_pool[i].data_request_buffer->same_page = true;
    this->c_cb_pool[i].data_response_buffer->head = 0;
    this->c_cb_pool[i].data_response_buffer->tail = 0;
    this->c_cb_pool[i].data_response_buffer->same_page = true;

    this->nr_write_response[i] = 0;
    this->nr_read_response[i] = 0;
    this->nr_write_request[i] = 0;
    this->nr_read_request[i] = 0;
    this->tmp_meta_data[i] =
        (struct meta_data *)malloc(sizeof(struct meta_data));
    this->time_comm_us[i] = 0;
    local_write_meta(i);
    std::deque<struct user_space_map> tmp_deque;
    user_space_queues.push_back(tmp_deque);
  }
}

uint64_t rdma_context::init_resources() {
  uint64_t ret, i;
  struct rdma_cm_id *id;
  ret = rdma_create_id(NULL, &this->srq_id, NULL, RDMA_PS_TCP);
  if (ret) {
    VERB_ERR("rdma_create_id", ret);
    return ret;
  }

  if (this->server == 0) {
    ret = rdma_resolve_addr(this->srq_id, NULL, this->rai->ai_dst_addr, 1000);
    if (ret) {
      VERB_ERR("rdma_resolve_addr", ret);
      return ret;
    }
  } else {
    ret = rdma_bind_addr(this->srq_id, this->rai->ai_src_addr);
    if (ret) {
      VERB_ERR("rdma_bind_addr", ret);
      return ret;
    }
  }

  this->recv_mr = rdma_reg_msgs(this->srq_id, this->recv_buf, this->msg_length);
  if (!this->recv_mr) {
    VERB_ERR("rdma_reg_msgs", -1);
    return -1;
  }
  this->send_mr = rdma_reg_msgs(this->srq_id, this->send_buf, this->msg_length);
  if (!this->send_mr) {
    VERB_ERR("rdma_reg_msgs", -1);
    return -1;
  }
  for (uint64_t i = 0; i < this->nr_workers; i++) {
    this->c_cb_pool[i].metadata_request_buffer->buffer_mr = rdma_reg_read(
        this->srq_id, this->c_cb_pool[i].metadata_request_buffer->data,
        COMPUTE_BUFFER_SIZE_METADATA);
    if (!this->c_cb_pool[i].metadata_request_buffer->buffer_mr) {
      VERB_ERR("rdma_reg_msgs", -1);
      return -1;
    }
    this->c_cb_pool[i].data_request_buffer->buffer_mr = rdma_reg_read(
        this->srq_id, this->c_cb_pool[i].data_request_buffer->data,
        COMPUTE_BUFFER_SIZE_DATA);
    if (!this->c_cb_pool[i].data_request_buffer->buffer_mr) {
      VERB_ERR("rdma_reg_msgs", -1);
      return -1;
    }
    this->c_cb_pool[i].metadata_response_buffer->buffer_mr = rdma_reg_write(
        this->srq_id, this->c_cb_pool[i].metadata_response_buffer->data, 16);
    if (!this->c_cb_pool[i].metadata_response_buffer->buffer_mr) {
      VERB_ERR("rdma_reg_msgs", -1);
      return -1;
    }
    this->c_cb_pool[i].data_response_buffer->buffer_mr = rdma_reg_write(
        this->srq_id, this->c_cb_pool[i].data_response_buffer->data,
        COMPUTE_BUFFER_SIZE_DATA);
    if (!this->c_cb_pool[i].data_response_buffer->buffer_mr) {
      VERB_ERR("rdma_reg_msgs", -1);
      return -1;
    }
  }

  struct ibv_srq_init_attr srq_attr;
  memset(&srq_attr, 0, sizeof(srq_attr));
  srq_attr.attr.max_wr = this->max_wr;
  srq_attr.attr.max_sge = 1;

  ret = rdma_create_srq(this->srq_id, NULL, &srq_attr);
  if (ret) {
    VERB_ERR("rdma_create_srq", ret);
    return -1;
  }

  this->srq = this->srq_id->srq;

  for (i = 0; i < this->max_wr; i++) {
    ret = rdma_post_recv(this->srq_id, NULL, this->recv_buf, this->msg_length,
                         this->recv_mr);
    if (ret) {
      VERB_ERR("rdma_post_recv", ret);
      return ret;
    }
  }

  this->srq_cq_channel = ibv_create_comp_channel(this->srq_id->verbs);
  if (!this->srq_cq_channel) {
    VERB_ERR("ibv_create_comp_channel", -1);
    return -1;
  }

  this->srq_cq = ibv_create_cq(this->srq_id->verbs, this->max_wr, NULL,
                               this->srq_cq_channel, 0);
  if (!this->srq_cq) {
    VERB_ERR("ibv_create_cq", -1);
    return -1;
  }

  ret = ibv_req_notify_cq(this->srq_cq, 0);
  if (ret) {
    VERB_ERR("ibv_req_notify_cq", ret);
    return ret;
  }

  return 0;
}

uint64_t rdma_context::init_connect() {
  uint64_t ret, i, ne;
  struct ibv_wc wc;
  struct ibv_qp_init_attr attr;

  ret = init_resources();
  if (ret) {
    VERB_ERR("init_resources", ret);
    return ret;
  }

  /* we need nr_threads + 1 id, the last one is for p4 switch */
  for (i = 0; i < this->nr_workers + 4; i++) {
    memset(&attr, 0, sizeof(attr));
    attr.qp_context = this;
    attr.cap.max_send_wr = this->max_wr;
    attr.cap.max_recv_wr = this->max_wr;
    attr.cap.max_send_sge = 20;
    attr.cap.max_recv_sge = 20;
    attr.cap.max_inline_data = 0;
    attr.recv_cq = this->srq_cq;
    attr.srq = this->srq;
    attr.sq_sig_all = 0;

    ret = rdma_create_ep(&this->conn_id[i], rai, NULL, &attr);
    if (ret) {
      VERB_ERR("rdma_create_ep", ret);
      return ret;
    }

    ret = rdma_connect(this->conn_id[i], NULL);
    if (ret) {
      VERB_ERR("rdma_connect", ret);
      return ret;
    }
  }
}

/* send the ibv_mr for all the buffers in the compute node to the remote
side. We only send the metadata buffers since the address info for data
buffers will be written in the requests. remote side doesn't need to know
the data buffers */
uint64_t rdma_context::send_buffer_mr() {

  uint64_t ret, i;
  struct ibv_wc wc;
  /* write buffer_mr into send buffer */
  for (i = 0; i < this->nr_workers; i++) {
    memcpy(this->send_buf + (i * 4) * sizeof(struct ibv_mr),
           this->c_cb_pool[i].metadata_request_buffer->buffer_mr,
           sizeof(struct ibv_mr));
    memcpy(this->send_buf + (i * 4 + 1) * sizeof(struct ibv_mr),
           this->c_cb_pool[i].metadata_response_buffer->buffer_mr,
           sizeof(struct ibv_mr));
    memcpy(this->send_buf + (i * 4 + 2) * sizeof(struct ibv_mr),
           this->c_cb_pool[i].data_request_buffer->buffer_mr,
           sizeof(struct ibv_mr));
    memcpy(this->send_buf + (i * 4 + 3) * sizeof(struct ibv_mr),
           this->c_cb_pool[i].data_response_buffer->buffer_mr,
           sizeof(struct ibv_mr));
  }

  ret = rdma_post_send(this->conn_id[0], NULL, this->send_buf, this->msg_length,
                       this->send_mr, IBV_SEND_SIGNALED);
  ret = rdma_get_send_comp(this->conn_id[0], &wc);
  if (ret <= 0) {
    VERB_ERR("rdma_get_send_comp", ret);
    return ret;
  }
  this->remote_buffer_mr = new struct ibv_mr();
  memset(this->remote_buffer_mr, 0, sizeof(struct ibv_mr));
  while (1) {
    memcpy(this->remote_buffer_mr,
           this->c_cb_pool[0].data_response_buffer->data,
           sizeof(struct ibv_mr));
    if (this->remote_buffer_mr->addr != 0) {
      break;
    }
  };
  this->remote_buffer_size = 4294967296ull;
}

} // namespace core
} // namespace FASTER
