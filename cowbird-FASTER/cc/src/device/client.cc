#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <libgen.h>
#include <rdma/rdma_cma.h>
#include <thread>
#include <vector>

#include "tbb/concurrent_unordered_map.h"
template <typename K, typename V>
using concurrent_unordered_map = tbb::concurrent_unordered_map<K, V>;

#include "tbb/concurrent_queue.h"
template <typename T>
using concurrent_queue = tbb::concurrent_queue<T>;

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)
#define BATCH_SIZE 4096
#define PAGE_SIZE 1048576
#define COMPUTE_BUFFER_SIZE 1073741824
#define WRITE_SIZE 1048576
#define DEFAULT_PORT "9400"
#define DEFAULT_MAX_WR 64
#define TIMEOUT_IN_MS 500 /* ms */

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_qp *qp;
  struct ibv_comp_channel *comp_channel;
  struct rdma_cm_id *listen_id;
  struct rdma_cm_id **conn_id;
  std::vector<std::thread> completion_worker;
};


struct meta_data {
  int rw_type; // 0 for read; 1 for write
  uint64_t req_addr;
  uint64_t resp_addr;
  uint32_t length;
};

struct LocalBuffer {
  struct ibv_mr *buffer_mr;
  void *data;
  int size;
  int head;
  int tail;
};



struct ClientBuffer {
  struct LocalBuffer *request_buffer;
  struct LocalBuffer *response_buffer;
};


//use in completion queue, wr_id
enum op_type {
  OP_RECV = 0,
  OP_READ,
  OP_WRITE
};





class RdmaClient{
    public:
      struct context *s_ctx;
      struct rdma_addrinfo *rai;

      int num_cb; // number of client buffers
      int num_workers;
      
      void *meta_send;
      struct ibv_mr *meta_send_mr;
      struct ClientBuffer *s_cb_pool;

      

      RdmaClient(){
        num_cb = 2;
        num_workers = 1;
        s_ctx = (struct context*)malloc(sizeof(struct context));
        s_ctx->conn_id = (struct rdma_cm_id **) calloc(num_workers,
                                                sizeof (struct rdma_cm_id *));
        memset(s_ctx->conn_id, 0, sizeof(s_ctx->conn_id));

        s_cb_pool = (struct ClientBuffer *)malloc(sizeof(struct ClientBuffer) * num_cb);
        
        for (int i = 0; i < num_cb; i++) {
          s_cb_pool[i].request_buffer = (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
          s_cb_pool[i].response_buffer = (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
          s_cb_pool[i].request_buffer->data = malloc(COMPUTE_BUFFER_SIZE);
          s_cb_pool[i].request_buffer->data = malloc(COMPUTE_BUFFER_SIZE);
        }
      }

      ~RdmaClient(){

      }

      void start(char *server_addr);
      int on_event(struct rdma_cm_event *event);
      int on_addr_resolved(struct rdma_cm_id *id);
      int on_route_resolved(struct rdma_cm_id *id);
      int on_connection(void *context);
      int on_disconnect(struct rdma_cm_id *id);
      void register_memory();
      void build_context(struct ibv_context *verbs);
      void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
      void poll_cq();
      void on_completion(struct ibv_wc *wc);

      

};



void RdmaClient::on_completion(struct ibv_wc *wc)
{
  if (wc->status != IBV_WC_SUCCESS)
    printf("on_completion: status is not IBV_WC_SUCCESS, status: %d.", wc->status);
  
  if (wc->opcode & IBV_WC_RECV) {
    // receive meta data
  }
  // process rdma read/write
}

void RdmaClient::poll_cq()
{
  struct ibv_cq *cq;
  struct ibv_wc wc;

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, NULL));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc))
      on_completion(&wc);
  }

}
void RdmaClient::register_memory(){
  
}
void RdmaClient::build_context(struct ibv_context *verbs)
{
  
  s_ctx->ctx = verbs;
  TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
  TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
  TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));
  
}

void RdmaClient::build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));
  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC;
  qp_attr->cap.max_send_wr = 10;
  qp_attr->cap.max_recv_wr = 10;
  qp_attr->cap.max_send_sge = 1;
  qp_attr->cap.max_recv_sge = 1;
}

int RdmaClient::on_addr_resolved(struct rdma_cm_id *id)
{
    struct ibv_qp_init_attr attr;
    printf("address resolved.\n");
    build_context(id->verbs);
    //build_qp_attr(&attr);
    /*for (int i = 0; i < num_workers; i++) {
        TEST_NZ(rdma_create_ep(&s_ctx->conn_id[i], rai, NULL, &attr));
    }*/
    register_memory();
    TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));
    
}
int RdmaClient::on_route_resolved(struct rdma_cm_id *id)
{
    printf("route resolved.\n");
    struct ibv_qp_init_attr attr;
    build_qp_attr(&attr);
    for (int i = 0; i < num_workers; i++) {
        TEST_NZ(rdma_create_ep(&s_ctx->conn_id[i], rai, NULL, &attr));
        TEST_NZ(rdma_connect(s_ctx->conn_id[i], NULL));
    }
    return 0;
}

int RdmaClient::on_connection(void *context)
{
  return 0;
}

int RdmaClient::on_disconnect(struct rdma_cm_id *id)
{
  return 0;
}

int RdmaClient::on_event(struct rdma_cm_event *event){
  int r = 0;
  printf("event is %d\n", event->event);
  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
    r = on_addr_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
    r = on_route_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = on_connection(event->id->context);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    r = on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
}

void RdmaClient::start(char *server_addr){

  struct addrinfo *addr;
  struct rdma_cm_event *event = NULL;
  struct rdma_event_channel *ec = NULL;
  struct rdma_addrinfo hints;
  memset(&hints, 0, sizeof (hints));
  
  TEST_NZ(rdma_getaddrinfo(server_addr, DEFAULT_PORT, &hints, &rai));
  TEST_NZ(getaddrinfo(server_addr, DEFAULT_PORT, NULL, &addr));
  TEST_Z(ec = rdma_create_event_channel());
  TEST_NZ(rdma_create_id(ec, &s_ctx->listen_id, NULL, RDMA_PS_TCP));
  TEST_NZ(rdma_resolve_addr(s_ctx->listen_id, NULL, addr->ai_addr, TIMEOUT_IN_MS));
  freeaddrinfo(addr);

  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    if (on_event(&event_copy))
      break;
  }
  free(rai);

  //rdma_destroy_id(s_ctx->listen_id);
  //rdma_destroy_event_channel(ec);
}






int main(int argc, char **argv)
{
  if (argc != 2)
    die("usage: client <server-address>");
  RdmaClient rs;
  rs.start(argv[1]);
  return 0;
}