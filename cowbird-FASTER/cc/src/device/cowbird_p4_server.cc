#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <errno.h>
#include <getopt.h>
#include <rdma/rdma_verbs.h>
#include <sched.h>
#include <stdlib.h>
#include <tbb/concurrent_queue.h>
#include <thread>
#include <unistd.h>
#include <vector>
template <typename T> using concurrent_queue = tbb::concurrent_queue<T>;

#define IB_ROCE_UDP_ENCAP_VALID_PORT_MIN (0xC000)
#define IB_ROCE_UDP_ENCAP_VALID_PORT_MAX (0xFFFF)
#define IB_GRH_FLOWLABEL_MASK (0x000FFFFF)

#define VERB_ERR(verb, ret)                                                    \
  fprintf(stderr, "%s returned %llu errno %llu\n", verb, ret, errno)

/* Default parameters values */
#define DEFAULT_PORT "51218"
#define DEFAULT_MSG_COUNT 10
#define DEFAULT_MSG_LENGTH 10000
#define DEFAULT_QP_COUNT 2
#define DEFAULT_THREADS 2
#define DEFAULT_MAX_WR 256

#define BATCH_SIZE 4096
#define PAGE_SIZE 4096
#define COMPUTE_BUFFER_SIZE 1073741824ull + 2 * sizeof(uint64_t)
#define WRITE_SIZE 4096
#define WRITE_BUFFER_SIZE 314572800ull
#define TIMEOUT_IN_MS 500 /* ms */
#define META_SIZE 2 * sizeof(uint64_t)

struct ClientBuffer {
  struct ibv_mr *metadata_request_mr;
  struct ibv_mr *data_request_mr;
  struct ibv_mr *metadata_response_mr;
  struct ibv_mr *data_response_mr;
};

struct context {
  /* User parameters */
  uint64_t server;
  char *server_name;
  char *server_port;
  uint64_t msg_length;
  uint64_t qp_count;
  uint64_t max_wr;

  /* Resources */
  struct rdma_cm_id *srq_id;
  struct rdma_cm_id *listen_id;
  struct rdma_cm_id **conn_id;
  struct ibv_mr *send_mr;
  struct ibv_mr *recv_mr;
  struct ibv_mr *buffer_mr;
  struct ibv_srq *srq;
  struct ibv_cq *srq_cq;
  struct ibv_comp_channel *srq_cq_channel;
  char *send_buf;
  char *recv_buf;
  struct ClientBuffer *s_cb_pool;

  uint64_t kSegmentSize;
  uint64_t kCapacity;
  uint64_t num_segments;
  uint8_t *buffer_pool;
};

uint64_t init_resources(struct context *ctx, struct rdma_addrinfo *rai) {
  uint64_t ret, i;
  struct rdma_cm_id *id;

  ret = rdma_create_id(NULL, &ctx->srq_id, NULL, RDMA_PS_TCP);
  if (ret) {
    VERB_ERR("rdma_create_id", ret);
    return ret;
  }

  if (ctx->server == 0) {
    ret = rdma_resolve_addr(ctx->srq_id, NULL, rai->ai_dst_addr, 1000);
    if (ret) {
      VERB_ERR("rdma_resolve_addr", ret);
      return ret;
    }
  } else {
    ret = rdma_bind_addr(ctx->srq_id, rai->ai_src_addr);
    if (ret) {
      VERB_ERR("rdma_bind_addr", ret);
      return ret;
    }
  }

  ctx->recv_mr = rdma_reg_msgs(ctx->srq_id, ctx->recv_buf, ctx->msg_length);
  if (!ctx->recv_mr) {
    VERB_ERR("rdma_reg_msgs", -1);
    return -1;
  }
  ctx->send_mr = rdma_reg_msgs(ctx->srq_id, ctx->send_buf, ctx->msg_length);
  if (!ctx->send_mr) {
    VERB_ERR("rdma_reg_msgs", -1);
    return -1;
  }

  ctx->buffer_mr = ibv_reg_mr(ctx->srq_id->pd, ctx->buffer_pool, ctx->kCapacity,
                              (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                               IBV_ACCESS_REMOTE_WRITE));
  if (!ctx->buffer_mr) {
    VERB_ERR("rdma_reg_msgs", -1);
    return ret;
  }

  struct ibv_srq_init_attr srq_attr;
  memset(&srq_attr, 0, sizeof(srq_attr));
  srq_attr.attr.max_wr = ctx->max_wr;
  srq_attr.attr.max_sge = 1;

  ret = rdma_create_srq(ctx->srq_id, NULL, &srq_attr);
  if (ret) {
    VERB_ERR("rdma_create_srq", ret);
    return -1;
  }

  ctx->srq = ctx->srq_id->srq;

  for (i = 0; i < ctx->max_wr; i++) {
    ret = rdma_post_recv(ctx->srq_id, NULL, ctx->recv_buf, ctx->msg_length,
                         ctx->recv_mr);
    if (ret) {
      VERB_ERR("rdma_post_recv", ret);
      return ret;
    }
  }

  ctx->srq_cq_channel = ibv_create_comp_channel(ctx->srq_id->verbs);
  if (!ctx->srq_cq_channel) {
    VERB_ERR("ibv_create_comp_channel", -1);
    return -1;
  }

  ctx->srq_cq = ibv_create_cq(ctx->srq_id->verbs, ctx->max_wr, NULL,
                              ctx->srq_cq_channel, 0);
  if (!ctx->srq_cq) {
    VERB_ERR("ibv_create_cq", -1);
    return -1;
  }

  ret = ibv_req_notify_cq(ctx->srq_cq, 0);
  if (ret) {
    VERB_ERR("ibv_req_notify_cq", ret);
    return ret;
  }

  return 0;
}

uint64_t await_completion(struct context *ctx) {
  uint64_t ret;
  struct ibv_cq *ev_cq;
  void *ev_ctx;

  ret = ibv_get_cq_event(ctx->srq_cq_channel, &ev_cq, &ev_ctx);
  if (ret) {
    VERB_ERR("ibv_get_cq_event", ret);
    return ret;
  }
  ibv_ack_cq_events(ev_cq, 1);

  ret = ibv_req_notify_cq(ctx->srq_cq, 0);
  if (ret) {
    VERB_ERR("ibv_req_notify_cq", ret);
    return ret;
  }

  return 0;
}

static void receive_mr(struct context *ctx, struct ibv_wc *wc) {
  if (wc->opcode & IBV_WC_RECV) {

    for (uint64_t i = 0; i < DEFAULT_QP_COUNT; i++) {
      memcpy(ctx->s_cb_pool[i].metadata_request_mr,
             ctx->recv_buf + (i * 4) * sizeof(struct ibv_mr),
             sizeof(struct ibv_mr));

      memcpy(ctx->s_cb_pool[i].metadata_response_mr,
             ctx->recv_buf + (i * 4 + 1) * sizeof(struct ibv_mr),
             sizeof(struct ibv_mr));

      memcpy(ctx->s_cb_pool[i].data_request_mr,
             ctx->recv_buf + (i * 4 + 2) * sizeof(struct ibv_mr),
             sizeof(struct ibv_mr));

      memcpy(ctx->s_cb_pool[i].data_response_mr,
             ctx->recv_buf + (i * 4 + 3) * sizeof(struct ibv_mr),
             sizeof(struct ibv_mr));
    }
  }
}

void rdma_write(struct context *ctx, uint64_t idx, int content) {
  uint64_t ret;
  struct ibv_wc wc;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  memset(&wr, 0, sizeof(wr));
  wr.wr_id = idx;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = (uintptr_t)ctx->s_cb_pool[0].data_response_mr->addr;
  wr.wr.rdma.rkey = ctx->s_cb_pool[0].data_response_mr->rkey;

  memcpy(ctx->buffer_pool, ctx->buffer_mr, sizeof(struct ibv_mr));
  sge.addr = (uintptr_t)ctx->buffer_mr->addr;
  sge.length = sizeof(struct ibv_mr);
  sge.lkey = ctx->buffer_mr->lkey;

         ctx->s_cb_pool[0].metadata_response_mr->addr);
         ret = ibv_post_send(ctx->conn_id[idx]->qp, &wr, &bad_wr);

         ret = rdma_get_send_comp(ctx->conn_id[idx], &wc);
         if (ret < 0) {
           VERB_ERR("rdma_get_send_comp", ret);
         }
}

int send_info_socket(char *server_name, char *server_port,
                     struct context *ctx) {
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    perror("socket");
    return -1;
  }

  struct hostent *h;
  if ((h = gethostbyname(server_name)) == 0) {
    perror("gethostbyname failed.\n");
    close(sockfd);
    return -1;
  }
  struct sockaddr_in servaddr;
  memset(&servaddr, 0, sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_port = htons(atoi(server_port));
  memcpy(&servaddr.sin_addr, h->h_addr, h->h_length);
  if (connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) != 0) {
    perror("connect");
    close(sockfd);
    return -1;
  }

  /* info we need to send to the control plane of switch */
  /* for each queue pair(not including the last one for p4 switch) */
  /* 1. compute_qp
     2. memory_qp
     3. compute psn
     4. memory psn
     5. compute udp src port
     6. memory udp src port
     7. compute meta req addr
     8. compute meta req rkey
     9. compute meta resp addr
     10. compute meta resp rkey
     11. compute data req rkey
     12. compute data resp rkey */
  /* p4 specific queue pair */
  /* 1. compute_qp
     2. memory_qp
     3. compute psn
     4. memory psn
     5. compute udp src port
     6. memory udp src port */
  /* other info */
  /* 1. remote memory pool addr
     2. remote memory pool rkey */

  struct ibv_qp_attr attr;
  struct ibv_qp_init_attr init_attr;
  int ret;
  char buffer[16];
  int iret;
  for (int idx = 0; idx < ctx->qp_count + 4; idx++) {
    ret = ibv_query_qp(ctx->conn_id[idx]->qp, &attr,
                       IBV_QP_RQ_PSN | IBV_QP_SQ_PSN | IBV_QP_DEST_QPN |
                           IBV_QP_PORT | IBV_QP_AV,
                       &init_attr);
    if (ret < 0) {
      perror("query");
      return -1;
    }
    printf("qp num is %x\n", attr.dest_qp_num);
    printf("port num is %d\n", attr.port_num);
    printf("rq_psn is %d\n", attr.rq_psn);
    printf("sq_psn is %d\n", attr.sq_psn);
    printf("src port is %x\n", rdma_get_src_port(ctx->conn_id[idx]));
    printf("dst port is %x\n", rdma_get_dst_port(ctx->conn_id[idx]));

    uint64_t compute_port = 0;
    compute_port = htons(rdma_get_src_port(ctx->conn_id[idx]) ^
                         rdma_get_dst_port(ctx->conn_id[idx]));
    compute_port = compute_port | 0xc000;
    printf("compute_port is %d\n", compute_port);

    uint64_t v = (uint64_t)ctx->conn_id[idx]->qp->qp_num * attr.dest_qp_num;
    v ^= v >> 20;
    v ^= v >> 40;
    uint32_t fl = (uint32_t)(v & IB_GRH_FLOWLABEL_MASK);
    uint32_t fl_low = fl & 0x03fff, fl_high = fl & 0xFC000;
    fl_low ^= fl_high >> 14;

    printf("udp port is %d\n", fl_low | IB_ROCE_UDP_ENCAP_VALID_PORT_MIN);

    /* 1. compute_qp */
    memset(buffer, 0, sizeof(buffer));
    sprintf(buffer, "%x", attr.dest_qp_num);
    if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
      perror("send");
      return -1;
    }
    /* 2. memory_qp */
    memset(buffer, 0, sizeof(buffer));
    sprintf(buffer, "%x", ctx->conn_id[idx]->qp->qp_num);
    if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
      perror("send");
      return -1;
    }
    /* 3. compute psn */
    memset(buffer, 0, sizeof(buffer));
    sprintf(buffer, "%x", attr.rq_psn);
    if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
      perror("send");
      return -1;
    }
    /* 4. memory psn */
    memset(buffer, 0, sizeof(buffer));
    sprintf(buffer, "%x", attr.sq_psn);
    if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
      perror("send");
      return -1;
    }
    /* 5. compute udp src port */
    memset(buffer, 0, sizeof(buffer));
    sprintf(buffer, "%x", compute_port);
    if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
      perror("send");
      return -1;
    }
    /* 6. memory udp src port */
    memset(buffer, 0, sizeof(buffer));
    sprintf(buffer, "%x", fl_low | IB_ROCE_UDP_ENCAP_VALID_PORT_MIN);
    if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
      perror("send");
      return -1;
    }
    if (idx < ctx->qp_count) {
      /* 7. compute meta req addr */
      memset(buffer, 0, sizeof(buffer));
      sprintf(buffer, "%llx", ctx->s_cb_pool[idx].metadata_request_mr->addr);
      if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
        perror("send");
        return -1;
      }
      /* 8. compute meta req rkey */
      memset(buffer, 0, sizeof(buffer));
      sprintf(buffer, "%x", ctx->s_cb_pool[idx].metadata_request_mr->rkey);
      if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
        perror("send");
        return -1;
      }
      /* 9. compute meta resp addr */
      memset(buffer, 0, sizeof(buffer));
      sprintf(buffer, "%llx", ctx->s_cb_pool[idx].metadata_response_mr->addr);
      if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
        perror("send");
        return -1;
      }
      /* 10. compute meta resp rkey */
      memset(buffer, 0, sizeof(buffer));
      sprintf(buffer, "%x", ctx->s_cb_pool[idx].metadata_response_mr->rkey);
      if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
        perror("send");
        return -1;
      }
      /* 11. compute data req rkey */
      memset(buffer, 0, sizeof(buffer));
      sprintf(buffer, "%x", ctx->s_cb_pool[idx].data_request_mr->rkey);
      if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
        perror("send");
        return -1;
      }
      /* 12. compute data resp rkey */
      memset(buffer, 0, sizeof(buffer));
      sprintf(buffer, "%x", ctx->s_cb_pool[idx].data_response_mr->rkey);
      if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
        perror("send");
        return -1;
      }
    }
  }
  /* 1. remote memory pool addr */
  memset(buffer, 0, sizeof(buffer));
  sprintf(buffer, "%llx", ctx->buffer_mr->addr);
  if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
    perror("send");
    return -1;
  }
  /* 2. remote memory pool rkey */
  memset(buffer, 0, sizeof(buffer));
  sprintf(buffer, "%x", ctx->buffer_mr->rkey);
  if ((iret = send(sockfd, buffer, 16, 0)) <= 0) {
    perror("send");
    return -1;
  }

  printf("after sending\n");

  close(sockfd);
}

uint64_t run_server(struct context *ctx, struct rdma_addrinfo *rai) {
  uint64_t ret, i;
  struct ibv_wc wc;
  struct ibv_qp_init_attr qp_attr;

  ret = init_resources(ctx, rai);
  if (ret) {
    printf("init_resources returned %llu\n", ret);
    return ret;
  }

  ctx->listen_id = ctx->srq_id;

  ret = rdma_listen(ctx->listen_id, 4);
  if (ret) {
    VERB_ERR("rdma_listen", ret);
    return ret;
  }

  for (i = 0; i < ctx->qp_count + 4; i++) {
    ret = rdma_get_request(ctx->listen_id, &ctx->conn_id[i]);
    if (ret) {
      VERB_ERR("rdma_get_request", ret);
      return ret;
    }

    memset(&qp_attr, 0, sizeof(qp_attr));

    qp_attr.qp_context = ctx;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = ctx->max_wr;
    qp_attr.cap.max_recv_wr = ctx->max_wr;
    qp_attr.cap.max_send_sge = 20;
    qp_attr.cap.max_recv_sge = 20;
    qp_attr.cap.max_inline_data = 0;
    qp_attr.recv_cq = ctx->srq_cq;
    qp_attr.srq = ctx->srq;
    qp_attr.sq_sig_all = 0;

    ret = rdma_create_qp(ctx->conn_id[i], NULL, &qp_attr);
    if (ret) {
      VERB_ERR("rdma_create_qp", ret);
      return ret;
    }

    /* Set the new connection to use our SRQ */
    ctx->conn_id[i]->srq = ctx->srq;

    ret = rdma_accept(ctx->conn_id[i], NULL);
    if (ret) {
      VERB_ERR("rdma_accept", ret);
      return ret;
    }
  }

  ret = await_completion(ctx);
  while (ibv_poll_cq(ctx->srq_cq, 1, &wc)) {
    receive_mr(ctx, &wc);
    break;
  }

  rdma_write(ctx, 1, 1);

  send_info_socket("158.130.4.216", "8887", ctx);

  while (1) {
  }
  return 0;
}

void destroy_resources(struct context *ctx) {
  uint64_t i;

  if (ctx->conn_id) {
    for (i = 0; i < ctx->qp_count + 4; i++) {
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

  if (ctx->recv_mr)
    rdma_dereg_mr(ctx->recv_mr);

  if (ctx->send_mr)
    rdma_dereg_mr(ctx->send_mr);

  if (ctx->recv_buf)
    free(ctx->recv_buf);

  if (ctx->send_buf)
    free(ctx->send_buf);

  if (ctx->srq_cq)
    ibv_destroy_cq(ctx->srq_cq);

  if (ctx->srq_cq_channel)
    ibv_destroy_comp_channel(ctx->srq_cq_channel);

  if (ctx->srq_id) {
    rdma_destroy_srq(ctx->srq_id);
    rdma_destroy_id(ctx->srq_id);
  }
}

int main(uint64_t argc, char **argv) {
  int ret, op;
  struct context ctx;
  struct rdma_addrinfo *rai, hints;

  memset(&ctx, 0, sizeof(ctx));
  memset(&hints, 0, sizeof(hints));

  ctx.server = 1;
  ctx.server_port = DEFAULT_PORT;
  ctx.msg_length = DEFAULT_MSG_LENGTH;
  ctx.qp_count = DEFAULT_QP_COUNT;
  ctx.max_wr = DEFAULT_MAX_WR;
  ctx.server_name = "10.1.1.8";

  hints.ai_port_space = RDMA_PS_TCP;
  if (ctx.server == 1)
    hints.ai_flags = RAI_PASSIVE; /* this makes it a server */

  ret = rdma_getaddrinfo(ctx.server_name, ctx.server_port, &hints, &rai);
  if (ret) {
    VERB_ERR("rdma_getaddrinfo", ret);
    exit(1);
  }

  /* allocate memory for our QPs and send/recv buffers */
  /* we allocate one more for p4 switch */
  ctx.conn_id = (struct rdma_cm_id **)calloc(ctx.qp_count + 4,
                                             sizeof(struct rdma_cm_id *));
  memset(ctx.conn_id, 0, sizeof(ctx.conn_id));
  ctx.send_buf = (char *)malloc(ctx.msg_length);
  memset(ctx.send_buf, 0, ctx.msg_length);
  ctx.recv_buf = (char *)malloc(ctx.msg_length);
  memset(ctx.recv_buf, 0, ctx.msg_length);

  ctx.s_cb_pool = (struct ClientBuffer *)malloc(sizeof(struct ClientBuffer) *
                                                DEFAULT_QP_COUNT);
  for (int i = 0; i < DEFAULT_QP_COUNT; i++) {
    ctx.s_cb_pool[i].metadata_request_mr =
        (struct ibv_mr *)malloc(sizeof(ibv_mr));
    ctx.s_cb_pool[i].metadata_response_mr =
        (struct ibv_mr *)malloc(sizeof(ibv_mr));
    ctx.s_cb_pool[i].data_request_mr = (struct ibv_mr *)malloc(sizeof(ibv_mr));
    ctx.s_cb_pool[i].data_response_mr = (struct ibv_mr *)malloc(sizeof(ibv_mr));
  }
  ctx.kCapacity = 34359738368ull;
  ctx.buffer_pool = (uint8_t *)malloc(sizeof(uint8_t) * ctx.kCapacity);

  ret = run_server(&ctx, rai);
  destroy_resources(&ctx);
  free(rai);
  return ret;
}
