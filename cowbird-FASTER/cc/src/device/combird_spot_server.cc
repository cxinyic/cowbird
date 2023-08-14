/*
 * Compile Command:
 * g++ -std=c++11 test_server.cc -o srq  -libverbs -lrdmacm -pthread -ltbb
 * 
 *
 * Server (-a is IP of local interface):
 * ./srq -s -a 192.168.1.12
 * 
 * Client (-a is IP of remote interface):
 * ./srq -a 192.168.1.12
 * 
 * buffer structure(for each thread):
 * COMPUTE SIDE: request_buffer; response_buffer 
 * read+write requests are written into request_buffer(compute node local)
 * the first META_SIZE of request_buffer: request_head; response_tail 
 * (updated by compute node; read by memory node)
 * 
 * read+write responses are written into response_buffer(memory node remote)
 * the first META_SIZE of response_buffer: request_tail; response_head
 * (updated by memory node; read by compute node)
 * 
 * MEMORY SIDE: recv_buffer; send_buffer;
 * recv_buffer: keep reading request_buffer until the request_tail >=
 * request_head; retrieve the request_head again
 * 
 * send_buffer: keep writing response_buffer in batches until the response_head <=
 * response_tail; retrieve the response_tail again
 * 
 * data can be written across the page_size; meta_data cannot be written across the page_size
 * 
 * 
 */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <getopt.h>
#include <rdma/rdma_verbs.h>
#include <thread>
#include <vector>
#include <unistd.h>
#include <tbb/concurrent_queue.h>
#include <sched.h>
template <typename T>
using concurrent_queue = tbb::concurrent_queue<T>;


#define VERB_ERR(verb, ret) \
        fprintf(stderr, "%s returned %llu errno %llu\n", verb, ret, errno)

/* Default parameters values */
#define DEFAULT_PORT "51218"
#define DEFAULT_MSG_COUNT 10
#define DEFAULT_MSG_LENGTH 100000
#define DEFAULT_QP_COUNT 16
#define DEFAULT_THREADS 16
#define DEFAULT_MAX_WR 64

#define BATCH_SIZE 3072*4
#define COMPUTE_BUFFER_SIZE 536870912ull + 2 * sizeof(uint64_t)
// #define COMPUTE_BUFFER_SIZE 8589934592ull + 2 * sizeof(uint64_t)
#define WRITE_SIZE 69632*4
#define WRITE_BUFFER_SIZE 314572800ull
#define TIMEOUT_IN_MS 500 /* ms */
#define META_SIZE 2*sizeof(uint64_t)

/*struct meta_data {
  uint32_t rw_type; // 0 for read; 1 for write
  uint64_t req_addr;
  uint64_t resp_addr;
  uint32_t length;
};*/
struct meta_data
{
      uint32_t rw_type; // 0 for read; 1 for write
      uint32_t length;
      uint64_t req_addr;
      uint64_t resp_addr;
};

struct write_across {
  struct meta_data *write_meta;
  uint64_t write_addr;
  uint64_t tail;
};

struct RemoteBuffer {
  struct ibv_mr *buffer_mr;
  uint64_t size;
  uint64_t head;
  uint64_t tail;
  bool same_page;
};

struct LocalBuffer {
  struct ibv_mr *buffer_mr;
  char *data;
  uint64_t tail;
};

struct ClientBuffer {
  struct RemoteBuffer *request_buffer;
  struct RemoteBuffer *response_buffer;
};

struct ServerBuffer {
  struct LocalBuffer *recv_buffer;
  struct LocalBuffer *send_buffer;
};

//use in completion queue, wr_id
enum op_type {
  OP_RECV = 0,
  OP_READ,
  OP_WRITE
};


/* Resources used in the example */
struct context
{
    /* User parameters */
    uint64_t server;
    char *server_name;
    char *server_port;
    uint64_t msg_count;
    uint64_t msg_length;
    uint64_t qp_count;
    uint64_t max_wr;

    /* Resources */
    struct rdma_cm_id *srq_id;
    struct rdma_cm_id *listen_id;
    struct rdma_cm_id **conn_id;
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;
    struct ibv_srq *srq;
    struct ibv_cq *srq_cq;
    struct ibv_comp_channel *srq_cq_channel;
    char *send_buf;
    char *recv_buf;
    struct ClientBuffer *s_cb_pool;
    struct ServerBuffer *s_sb_pool;

    uint64_t kSegmentSize;
    uint64_t kCapacity;
    uint64_t num_segments;
    uint8_t ** buffer_pool;
    bool *write_flags;
    struct write_across *write_across_metas;
    std::vector<std::thread> workers;
    uint64_t* nr_process_read; 
    uint64_t* nr_process_write;
};

concurrent_queue<uint64_t> tasks;

void SetThreadAffinity(size_t core) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(core, &mask);
    sched_setaffinity(0, sizeof(mask), &mask);
}

/*
 * Function: init_resources
 * 
 * Input:
 *      ctx     The context object
 *      rai     The RDMA address info for the connection
 * 
 * Output:
 *      none
 * 
 * Returns:
 *      0 on success, non-zero on failure
 * 
 * Description:
 *      This function initializes resources that are common to both the client
 *      and server functionality. 
 *      It creates our SRQ, registers memory regions, posts receive buffers 
 *      and creates a single completion queue that will be used for the receive 
 *      queue on each queue pair.
 */
uint64_t init_resources(struct context *ctx, struct rdma_addrinfo *rai)
{
    uint64_t ret, i;
    struct rdma_cm_id *id;

    /* Create an ID used for creating/accessing our SRQ */
    ret = rdma_create_id(NULL, &ctx->srq_id, NULL, RDMA_PS_TCP);
    if (ret) {
        VERB_ERR("rdma_create_id", ret);
        return ret;
    }

    /* We need to bind the ID to a particular RDMA device
     * This is done by resolving the address or binding to the address */
    if (ctx->server == 0) {
        ret = rdma_resolve_addr(ctx->srq_id, NULL, rai->ai_dst_addr, 1000);
        if (ret) {
            VERB_ERR("rdma_resolve_addr", ret);
            return ret;
        }
    }
    else {
        ret = rdma_bind_addr(ctx->srq_id, rai->ai_src_addr);
        if (ret) {
            VERB_ERR("rdma_bind_addr", ret);
            return ret;
        }
    }

    /* Create the memory regions being used in this example */
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

    for (uint64_t i = 0; i < DEFAULT_QP_COUNT; i++) {
        ctx->s_sb_pool[i].recv_buffer->buffer_mr = rdma_reg_msgs(ctx->srq_id, ctx->s_sb_pool[i].recv_buffer->data, BATCH_SIZE);
        if (!ctx->s_sb_pool[i].recv_buffer->buffer_mr) {
            VERB_ERR("rdma_reg_msgs", -1);
            return -1;
        }
        ctx->s_sb_pool[i].send_buffer->buffer_mr = rdma_reg_msgs(ctx->srq_id, ctx->s_sb_pool[i].send_buffer->data, WRITE_BUFFER_SIZE);
        if (!ctx->s_sb_pool[i].send_buffer->buffer_mr) {
            VERB_ERR("rdma_reg_msgs", -1);
            return -1;
        }
    }

    /* Create our shared receive queue */
    struct ibv_srq_init_attr srq_attr;
    memset(&srq_attr, 0, sizeof (srq_attr));
    srq_attr.attr.max_wr = ctx->max_wr;
    srq_attr.attr.max_sge = 1;

    ret = rdma_create_srq(ctx->srq_id, NULL, &srq_attr);
    if (ret) {
        VERB_ERR("rdma_create_srq", ret);
        return -1;
    }

    /* Save the SRQ in our context so we can assign it to other QPs later */
    ctx->srq = ctx->srq_id->srq;

    /* Post our receive buffers on the SRQ */
    for (i = 0; i < ctx->max_wr; i++) {
        ret = rdma_post_recv(ctx->srq_id, NULL, ctx->recv_buf, ctx->msg_length,
                             ctx->recv_mr);
        if (ret) {
            VERB_ERR("rdma_post_recv", ret);
            return ret;
        }
    }

    /* Create a completion channel to use with the SRQ CQ */
    ctx->srq_cq_channel = ibv_create_comp_channel(ctx->srq_id->verbs);
    if (!ctx->srq_cq_channel) {
        VERB_ERR("ibv_create_comp_channel", -1);
        return -1;
    }

    /* Create a CQ to use for all connections (QPs) that use the SRQ */
    ctx->srq_cq = ibv_create_cq(ctx->srq_id->verbs, ctx->max_wr, NULL,
                                ctx->srq_cq_channel, 0);
    if (!ctx->srq_cq) {
        VERB_ERR("ibv_create_cq", -1);
        return -1;
    }

    /* Make sure that we get notified on the first completion */
    ret = ibv_req_notify_cq(ctx->srq_cq, 0);
    if (ret) {
        VERB_ERR("ibv_req_notify_cq", ret);
        return ret;
    }

    return 0;
}

/*
 * Function:    destroy_resources
 * 
 * Input:
 *      ctx     The context object
 * 
 * Output:
 *      none
 * 
 * Returns:
 *      0 on success, non-zero on failure
 * 
 * Description:
 *      This function cleans up resources used by the application
 */
void destroy_resources(struct context *ctx)
{
    uint64_t i;

    if (ctx->conn_id) {
        for (i = 0; i < ctx->qp_count; i++) {
            if (ctx->conn_id[i]) {
                if (ctx->conn_id[i]->qp &&
                    ctx->conn_id[i]->qp->state == IBV_QPS_RTS) {
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
    for (uint64_t i = 0; i < DEFAULT_QP_COUNT; i++) {
        if (ctx->s_sb_pool[i].recv_buffer->buffer_mr)
            rdma_dereg_mr(ctx->s_sb_pool[i].recv_buffer->buffer_mr);
        if (ctx->s_sb_pool[i].send_buffer->buffer_mr)
            rdma_dereg_mr(ctx->s_sb_pool[i].send_buffer->buffer_mr);
    }
}

/*
 * Function:    await_completion
 * 
 * Input:
 *      ctx     The context object
 * 
 * Output:
 *      none
 * 
 * Returns:
 *      0 on success, non-zero on failure
 * 
 * Description:
 *      Waits for a completion on the SRQ CQ
 * 
 */
uint64_t await_completion(struct context *ctx)
{
    uint64_t ret;
    struct ibv_cq *ev_cq;
    void *ev_ctx;

    /* Wait for a CQ event to arrive on the channel */
    ret = ibv_get_cq_event(ctx->srq_cq_channel, &ev_cq, &ev_ctx);
    if (ret) {
        VERB_ERR("ibv_get_cq_event", ret);
        return ret;
    }
    ibv_ack_cq_events(ev_cq, 1);
    /* Reload the event notification */
    ret = ibv_req_notify_cq(ctx->srq_cq, 0);
    if (ret) {
        VERB_ERR("ibv_req_notify_cq", ret);
        return ret;
    }

    return 0;
}

static void receive_mr(struct context * ctx, struct ibv_wc *wc)
{
    if (wc->opcode & IBV_WC_RECV){
        printf("receive meta\n");
        for (uint64_t i = 0; i < DEFAULT_QP_COUNT; i++) {
            memcpy(ctx->s_cb_pool[i].request_buffer->buffer_mr, ctx->recv_buf + (i*2) * sizeof(struct ibv_mr), sizeof(struct ibv_mr));
            memcpy(ctx->s_cb_pool[i].response_buffer->buffer_mr, ctx->recv_buf + (i*2+1) * sizeof(struct ibv_mr), sizeof(struct ibv_mr));
            printf("request buffer addr is %llu\n", ctx->s_cb_pool[i].request_buffer->buffer_mr->addr);
        }
        printf("after receive meta\n");
        
    }

}

static void receive_head(struct context * ctx, struct ibv_wc *wc)
{
    if (wc->opcode & IBV_WC_RECV){
        printf("receive head\n");
        uint64_t *tmp = new uint64_t();
        for (uint64_t i = 0; i < DEFAULT_QP_COUNT; i++) {
            memcpy(tmp, ctx->recv_buf + i* sizeof(uint64_t), sizeof(uint64_t));
            ctx->s_cb_pool[i].request_buffer->head = *tmp;
        }
        printf("after receive head: %llu\n", ctx->s_cb_pool[0].request_buffer->head);
        
    }

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
    wr.wr.rdma.remote_addr = (uintptr_t)ctx->s_cb_pool[idx].request_buffer->buffer_mr->addr;
    wr.wr.rdma.rkey = ctx->s_cb_pool[idx].request_buffer->buffer_mr->rkey;

    sge.addr = (uintptr_t)ctx->s_sb_pool[idx].recv_buffer->data;
    sge.length = META_SIZE;
    sge.lkey = ctx->s_sb_pool[idx].recv_buffer->buffer_mr->lkey;

    ret = ibv_post_send(ctx->conn_id[idx]->qp, &wr, &bad_wr);  
    ret = rdma_get_send_comp(ctx->conn_id[idx], &wc);    

    uint64_t *tmp = new uint64_t();
    // request head
    memcpy(tmp, ctx->s_sb_pool[idx].recv_buffer->data, sizeof(uint64_t));
    ctx->s_cb_pool[idx].request_buffer->head = *tmp;

    // response tail
    memcpy(tmp, ctx->s_sb_pool[idx].recv_buffer->data + sizeof(uint64_t), sizeof(uint64_t));
    ctx->s_cb_pool[idx].response_buffer->tail = *tmp;
    
    
}

void rdma_write_meta(struct context *ctx, uint64_t idx) {
    uint64_t ret;
    struct ibv_wc wc;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof(wr));
    uint64_t *tmp = new uint64_t();
    // request tail
    *tmp = ctx->s_cb_pool[idx].request_buffer->tail;
    memcpy(ctx->s_sb_pool[idx].send_buffer->data, tmp, sizeof(uint64_t));

    // response head
    *tmp = ctx->s_cb_pool[idx].response_buffer->head;
    memcpy(ctx->s_sb_pool[idx].send_buffer->data + sizeof(uint64_t), tmp, sizeof(uint64_t));
    
    wr.wr_id = idx;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = (uintptr_t)ctx->s_cb_pool[idx].response_buffer->buffer_mr->addr;
    wr.wr.rdma.rkey = ctx->s_cb_pool[idx].response_buffer->buffer_mr->rkey;

    sge.addr = (uintptr_t)ctx->s_sb_pool[idx].send_buffer->data;
    sge.length = META_SIZE;
    sge.lkey = ctx->s_sb_pool[idx].send_buffer->buffer_mr->lkey;

    ret = ibv_post_send(ctx->conn_id[idx]->qp, &wr, &bad_wr);  
    ret = rdma_get_send_comp(ctx->conn_id[idx], &wc); 
    // printf("response head is %llu\n", ctx->s_cb_pool[idx].response_buffer->head);   
    
}

void rdma_read(struct context *ctx, uint64_t idx){
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
    wr.wr.rdma.remote_addr = (uintptr_t)ctx->s_cb_pool[idx].request_buffer->buffer_mr->addr + ctx->s_cb_pool[idx].request_buffer->tail;
    wr.wr.rdma.rkey = ctx->s_cb_pool[idx].request_buffer->buffer_mr->rkey;

    sge.addr = (uintptr_t)ctx->s_sb_pool[idx].recv_buffer->data;
    sge.length = BATCH_SIZE;
    sge.lkey = ctx->s_sb_pool[idx].recv_buffer->buffer_mr->lkey;

    ret = ibv_post_send(ctx->conn_id[idx]->qp, &wr, &bad_wr);  
    ret = rdma_get_send_comp(ctx->conn_id[idx], &wc);    
    ctx->s_cb_pool[idx].request_buffer->tail += BATCH_SIZE;
    // turn to the next page, within same page again
    if (ctx->s_cb_pool[idx].request_buffer->tail >= COMPUTE_BUFFER_SIZE) {
        ctx->s_cb_pool[idx].request_buffer->tail -= (COMPUTE_BUFFER_SIZE - 2*sizeof(uint64_t));
        ctx->s_cb_pool[idx].request_buffer->same_page = true;
    }
    //printf("ctx->conn_id[%llu], after rdma_get_send_comp, wc->opcode is %llu\n", idx, wc.opcode); 

}
bool pre_rdma_write(struct context *ctx, uint64_t idx, uint64_t length){
    uint64_t prev_response_tail = ctx->s_cb_pool[idx].response_buffer->tail;
    if (ctx->s_cb_pool[idx].response_buffer->same_page == true && 
        ctx->s_cb_pool[idx].response_buffer->head + length > COMPUTE_BUFFER_SIZE) {
        rdma_read_meta(ctx, idx);  
        if (ctx->s_cb_pool[idx].response_buffer->head + length - COMPUTE_BUFFER_SIZE
            > ctx->s_cb_pool[idx].response_buffer->tail){
            return false;
        }
        else {
            ctx->s_cb_pool[idx].response_buffer->same_page = false; 
            return true;
        }
    }
    if (ctx->s_cb_pool[idx].response_buffer->same_page == false && 
        ctx->s_cb_pool[idx].response_buffer->head + length > ctx->s_cb_pool[idx].response_buffer->tail) {
        rdma_read_meta(ctx, idx);  
        if (ctx->s_cb_pool[idx].response_buffer->tail != prev_response_tail){
            if (ctx->s_cb_pool[idx].response_buffer->head > ctx->s_cb_pool[idx].response_buffer->tail)
            {
                ctx->s_cb_pool[idx].response_buffer->same_page = true;
            }
            else
            {
                ctx->s_cb_pool[idx].response_buffer->same_page = false;
            }
        } 
        if (ctx->s_cb_pool[idx].response_buffer->same_page == false && 
           ctx->s_cb_pool[idx].response_buffer->head + length > ctx->s_cb_pool[idx].response_buffer->tail){
            return false;
        } 
        else{
            return true;
        } 
        
    }  
    return true; 
}
void rdma_write(struct context *ctx, uint64_t idx){
    // printf("rdma_write: ctx->s_cb_pool[idx].response_buffer->head is %llu\n", ctx->s_cb_pool[idx].response_buffer->head);
    // printf("rdma_write: ctx->s_cb_pool[idx].response_buffer->tail is %llu\n", ctx->s_cb_pool[idx].response_buffer->tail);
    while(!pre_rdma_write(ctx, idx, ctx->s_sb_pool[idx].send_buffer->tail)) { 
        usleep(500);
        // printf("waiting to write\n");
    }
    
    
    
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
    wr.wr.rdma.remote_addr = (uintptr_t)ctx->s_cb_pool[idx].response_buffer->buffer_mr->addr + ctx->s_cb_pool[idx].response_buffer->head;
    wr.wr.rdma.rkey = ctx->s_cb_pool[idx].response_buffer->buffer_mr->rkey;

    sge.addr = (uintptr_t)ctx->s_sb_pool[idx].send_buffer->data;
    sge.length = WRITE_SIZE;
    sge.lkey = ctx->s_sb_pool[idx].send_buffer->buffer_mr->lkey;

    ret = ibv_post_send(ctx->conn_id[idx]->qp, &wr, &bad_wr);  
    ret = rdma_get_send_comp(ctx->conn_id[idx], &wc);   
    ctx->s_cb_pool[idx].response_buffer->head += WRITE_SIZE;
    // turn to next page, not within same page
    if (ctx->s_cb_pool[idx].response_buffer->head >= COMPUTE_BUFFER_SIZE) {
        ctx->s_cb_pool[idx].response_buffer->head -= (COMPUTE_BUFFER_SIZE - 2*sizeof(uint64_t));
        ctx->s_cb_pool[idx].response_buffer->same_page = false;
    }
    memset(ctx->s_sb_pool[idx].send_buffer->data, 0, WRITE_SIZE);
    // TODO: when to update meta info to compute node?
    // printf("to do rdma_write_meta\n");
    rdma_write_meta(ctx, idx);


}

int num = 0;
void process_RW(struct context * ctx, uint64_t idx)
{
    num += 1;
    uint64_t cur_process = 0;
    void *read_address;
    void *write_address;
    uint64_t addr;
    uint64_t segment;
    struct meta_data *tmp_meta_data = new struct meta_data();
    uint64_t rest_length;
    // across page write request before
    // printf("ctx->write_flags[idx] is %llu\n", ctx->write_flags[idx]);
    // printf("prev ctx->write_across_metas[idx].tail is %llu\n", ctx->write_across_metas[idx].tail);
    if (ctx->write_flags[idx] == true){
        // check if we can finish the write with this batch
        if (ctx->write_across_metas[idx].write_meta->length - ctx->write_across_metas[idx].tail <= BATCH_SIZE){
            // flush if we have reached the WRITE_SIZE: meta_data
            
            if (ctx->s_sb_pool[idx].send_buffer->tail + sizeof(struct meta_data) > WRITE_SIZE){
                rdma_write(ctx, idx);
                ctx->s_sb_pool[idx].send_buffer->tail = 0;
            }
            // write the rest part to the write_address
            memcpy(reinterpret_cast<void *>(ctx->write_across_metas[idx].write_addr + ctx->write_across_metas[idx].tail), ctx->s_sb_pool[idx].recv_buffer->data + cur_process, ctx->write_across_metas[idx].write_meta->length - ctx->write_across_metas[idx].tail);
            // write back meta data
            memcpy(ctx->s_sb_pool[idx].send_buffer->data + ctx->s_sb_pool[idx].send_buffer->tail, ctx->write_across_metas[idx].write_meta, sizeof(struct meta_data));
            ctx->s_sb_pool[idx].send_buffer->tail += sizeof(struct meta_data);
            // printf("ctx->s_sb_pool[idx].send_buffer->tail is %llu\n", ctx->s_sb_pool[idx].send_buffer->tail);
            // clean write across flags
            ctx->write_flags[idx] = false;
            cur_process += ctx->write_across_metas[idx].write_meta->length - ctx->write_across_metas[idx].tail;
            // printf("step 4 cur_process is %llu\n", cur_process);
            // printf("can finish the write, cur_process is %llu, tail is %llu\n", cur_process, ctx->write_across_metas[idx].tail);
            ctx->write_across_metas[idx].tail = 0;
            ctx->nr_process_write[idx] += 1;
        }
        else {
            // write the rest part to the write_address
            memcpy(reinterpret_cast<void *>(ctx->write_across_metas[idx].write_addr + ctx->write_across_metas[idx].tail), ctx->s_sb_pool[idx].recv_buffer->data + cur_process, BATCH_SIZE);
            ctx->write_across_metas[idx].tail += BATCH_SIZE;
            cur_process = BATCH_SIZE;
        }
    }
    // no across page write request before
    if (ctx->write_flags[idx] == false){
        while (cur_process + sizeof(struct meta_data) <= BATCH_SIZE) {
            // printf("cur_process is %llu\n", cur_process);
            memcpy(tmp_meta_data, (void *)(ctx->s_sb_pool[idx].recv_buffer->data + cur_process), sizeof(struct meta_data));
            cur_process += sizeof(struct meta_data);
            
            //printf("prev ctx->write_across_metas[idx].tail is %llu\n", ctx->write_across_metas[idx].tail);
            // read request; process and write back immediately
            // if (num%100 ==0)
            // printf("tmp_meta_data->rw_type is %d, num is %d, req addr is %llu, resp addr is %llu, length is %d\n", tmp_meta_data->rw_type, num, tmp_meta_data->req_addr, tmp_meta_data->resp_addr, tmp_meta_data->length);
            if (tmp_meta_data->rw_type == 0){
                
                addr = tmp_meta_data->req_addr;
                segment = addr / ctx->kSegmentSize;
                read_address = ctx->buffer_pool[segment] + addr % ctx->kSegmentSize;
                
                // flush if we have reached the WRITE_SIZE: meta_data
                
                if (ctx->s_sb_pool[idx].send_buffer->tail + sizeof(struct meta_data) > WRITE_SIZE){
                    
                    rdma_write(ctx, idx);
                    ctx->s_sb_pool[idx].send_buffer->tail = 0;
                }
                
                    
                // write back meta data
                // printf("read idx %llu, step 1.5\n", idx);
                ctx->nr_process_read[idx] += 1;
                // printf("process_RW: idx %llu, ctx->nr_process_read is %llu\n", idx, ctx->nr_process_read[idx]);
                memcpy(ctx->s_sb_pool[idx].send_buffer->data + ctx->s_sb_pool[idx].send_buffer->tail, tmp_meta_data, sizeof(struct meta_data));
                ctx->s_sb_pool[idx].send_buffer->tail += sizeof(struct meta_data);
                // flush if we have reached the WRITE_SIZE: data; 
                // we have to write data across the page in this case
                if (ctx->s_sb_pool[idx].send_buffer->tail + tmp_meta_data->length > WRITE_SIZE){
                    // local read
                    // printf("idx %llu, process_RW read step1\n", idx);
                    memcpy(ctx->s_sb_pool[idx].send_buffer->data + ctx->s_sb_pool[idx].send_buffer->tail,  read_address, tmp_meta_data->length);        
                    rest_length = tmp_meta_data->length - (WRITE_SIZE - ctx->s_sb_pool[idx].send_buffer->tail);
                    ctx->s_sb_pool[idx].send_buffer->tail = WRITE_SIZE;
                    rdma_write(ctx, idx);
                    ctx->s_sb_pool[idx].send_buffer->tail = 0;
                    uint64_t i = 1;
                    // write all the data back; until it cannot fill the WRITE_SIZE
                    while (rest_length > WRITE_SIZE){
                        // printf("idx %llu, process_RW read step2\n", idx);
                        memcpy(ctx->s_sb_pool[idx].send_buffer->data + 0, ctx->s_sb_pool[idx].send_buffer->data + i * WRITE_SIZE, WRITE_SIZE);
                        ctx->s_sb_pool[idx].send_buffer->tail = WRITE_SIZE;
                        rdma_write(ctx, idx);
                        ctx->s_sb_pool[idx].send_buffer->tail = 0;
                        i += 1;
                        rest_length -= WRITE_SIZE;
                    }
                    if (rest_length > 0)
                    {   // printf("idx %llu, process_RW read step3\n", idx);
                        memcpy(ctx->s_sb_pool[idx].send_buffer->data + 0, ctx->s_sb_pool[idx].send_buffer->data + i * WRITE_SIZE, rest_length);
                        ctx->s_sb_pool[idx].send_buffer->tail = rest_length;
                    }
                }
                // local read
                else {
                    // printf("idx %llu, process_RW read step4, cur_process is %llu\n", idx, cur_process);
                    memcpy(ctx->s_sb_pool[idx].send_buffer->data + ctx->s_sb_pool[idx].send_buffer->tail, read_address, tmp_meta_data->length);        
                    // printf("idx %llu, after process_RW read step4\n", idx);
                    ctx->s_sb_pool[idx].send_buffer->tail += tmp_meta_data->length;
                    // printf("idx %llu, after process_RW read step5\n", idx);
                }
                
                
            } 
            //write request; process and write back in batch
            else {
                // printf("step 2: write request\n");
                addr = tmp_meta_data->req_addr;
                // printf("step 2.1: write request, addr is %llu\n", addr);
                uint64_t segment = addr / ctx->kSegmentSize;
                write_address = ctx->buffer_pool[segment] + addr % ctx->kSegmentSize;
                // printf("step 3: write request addr is %llu\n", write_address);
                // flush if we have reached the WRITE_SIZE: meta_data
                if (ctx->s_sb_pool[idx].send_buffer->tail + sizeof(struct meta_data) > WRITE_SIZE){
                    rdma_write(ctx, idx);
                    ctx->s_sb_pool[idx].send_buffer->tail = 0;
                }

                // write request across the pages
                if (tmp_meta_data->length + cur_process > BATCH_SIZE) {
                    if (BATCH_SIZE - cur_process > 0)
                        // write a part to the write_address
                        memcpy(reinterpret_cast<void *>(write_address + ctx->write_across_metas[idx].tail), (void*)(ctx->s_sb_pool[idx].recv_buffer->data + cur_process), BATCH_SIZE - cur_process);
                    
                    // store the write meta info, for the future write request
                    ctx->write_flags[idx] = true;
                    ctx->write_across_metas[idx].write_addr = (uintptr_t)write_address;
                    memcpy(ctx->write_across_metas[idx].write_meta, tmp_meta_data, sizeof(struct meta_data));
                    // printf("prev ctx->write_across_metas[idx].tail is %llu, BATCH_SIZE is %llu\n", ctx->write_across_metas[idx].tail , BATCH_SIZE);
                    ctx->write_across_metas[idx].tail += BATCH_SIZE - cur_process;
                    // printf("cur_process is %llu, ctx->write_across_metas[idx].tail is %llu\n", cur_process, ctx->write_across_metas[idx].tail);
                    cur_process = BATCH_SIZE;
                    // printf("step 2 cur_process is %llu\n", cur_process);
                }
                else {
                    // write request within the page
                    // printf("write within the page\n");
                    memcpy(write_address , ctx->s_sb_pool[idx].recv_buffer->data + cur_process, tmp_meta_data->length);
                    // write back meta data
                    memcpy(ctx->s_sb_pool[idx].send_buffer->data + ctx->s_sb_pool[idx].send_buffer->tail, tmp_meta_data, sizeof(struct meta_data));
                    ctx->s_sb_pool[idx].send_buffer->tail += sizeof(struct meta_data);
                    // printf("ctx->s_sb_pool[idx].send_buffer->tail is %llu\n", ctx->s_sb_pool[idx].send_buffer->tail);
                    // clean write across flags
                    ctx->write_flags[idx] = false;
                    ctx->write_across_metas[idx].tail = 0;
                    cur_process += tmp_meta_data->length;
                    ctx->nr_process_write[idx] += 1;
                    //printf("step 3 cur_process is %llu\n", cur_process);
                }
            }
        }
    }
    
}
bool pre_rdma_read(struct context *ctx, uint64_t idx, uint64_t length){
    uint64_t prev_request_head;
    prev_request_head = ctx->s_cb_pool[idx].request_buffer->head;
    if (ctx->s_cb_pool[idx].request_buffer->same_page == true && 
        ctx->s_cb_pool[idx].request_buffer->tail + length> ctx->s_cb_pool[idx].request_buffer->head) {
        rdma_read_meta(ctx, idx);  
        // printf("prev_request_head is %llu\n", prev_request_head);
        // printf("request head is %llu\n", ctx->s_cb_pool[idx].request_buffer->head);
        // printf("request tail is %llu\n", ctx->s_cb_pool[idx].request_buffer->tail);
                
        // if the head is the same: no new request
        if (ctx->s_cb_pool[idx].request_buffer->head != prev_request_head){
            if (ctx->s_cb_pool[idx].request_buffer->head > ctx->s_cb_pool[idx].request_buffer->tail)
            {
                ctx->s_cb_pool[idx].request_buffer->same_page = true;
            }
            else
            {
                ctx->s_cb_pool[idx].request_buffer->same_page = false;
            }
        }     
    }
    if (ctx->s_cb_pool[idx].request_buffer->same_page == true && 
        ctx->s_cb_pool[idx].request_buffer->tail + length > ctx->s_cb_pool[idx].request_buffer->head) {
        // printf("cannot read, tail: %d, head: %d, length: %d\n", ctx->s_cb_pool[idx].request_buffer->tail, ctx->s_cb_pool[idx].request_buffer->head, length);
        return false;
    }
    else {
        //printf("ctx->s_cb_pool[idx].request_buffer->same_page == true : %llu\n", ctx->s_cb_pool[idx].request_buffer->same_page);
        return true;
    }
    //printf("step2\n");
    return true;
}


void  handle_RW(struct context * ctx, uint64_t idx)
{   
    uint64_t prev_request_head;
    uint64_t i = 0 ; 
    uint64_t task_id;
    // SetThreadAffinity(idx);
    // task_id = idx;
    while(1){
        if (!tasks.try_pop(task_id)) {
            // usleep(500);
            continue;
        }
        
        if (!pre_rdma_read(ctx, task_id, BATCH_SIZE)) {
            // usleep(500);
            tasks.push(task_id);
            continue;
        }
        
        //printf("idx %llu, ctx->s_cb_pool[idx].request_buffer->head is %llu\n", idx, ctx->s_cb_pool[idx].request_buffer->head);
        // normal read
        i += 1;
        //printf("begin normal read, i is %llu\n", i);
        
        rdma_read(ctx, task_id);
        //printf("after normal read, ctx->s_cb_pool[idx].request_buffer->tail is %llu\n", ctx->s_cb_pool[idx].request_buffer->tail);
        //printf("prev ctx->write_across_metas[idx].tail is %llu\n", ctx->write_across_metas[idx].tail);
        // process the info
        process_RW(ctx, task_id);
        
        if (i%100000 == 0){
            printf("nr_process_read is %llu\n", ctx->nr_process_read[task_id]);
            printf("nr_process_write is %llu\n", ctx->nr_process_write[task_id]);
            printf("after process_RW, ctx->s_cb_pool[task_id].response_buffer->head is %llu\n", ctx->s_cb_pool[task_id].response_buffer->head);  
            printf("after process_RW, ctx->s_cb_pool[task_id].request_buffer->head is %llu\n", ctx->s_cb_pool[task_id].request_buffer->head);        
            printf("after process_RW, ctx->s_cb_pool[task_id].request_buffer->tail is %llu\n", ctx->s_cb_pool[task_id].request_buffer->tail);                    

        }
        tasks.push(task_id);
        
        // rdma_write(ctx, idx);
        // printf("nr_process_read is %llu\n", ctx->nr_process_read[idx]);
        // printf("nr_process_write is %llu\n", ctx->nr_process_write[idx]);
        // printf("after process_RW, ctx->s_cb_pool[idx].response_buffer->head is %llu\n", ctx->s_cb_pool[idx].response_buffer->head);  
        // printf("after process_RW, ctx->s_cb_pool[idx].request_buffer->head is %llu\n", ctx->s_cb_pool[idx].request_buffer->head);              
        // if (i > 5)
        //     break;

    }

    
   
}

/*
 * Function:    run_server
 * 
 * Input:
 *      ctx     The context object
 *      rai     The RDMA address info for the connection
 * 
 * Output:
 *      none
 * 
 * Returns:
 *      0 on success, non-zero on failure
 * 
 * Description:
 *      Executes the server side of the example
 */
uint64_t run_server(struct context *ctx, struct rdma_addrinfo *rai)
{
    uint64_t ret, i;
    uint64_t send_count = 0;
    uint64_t recv_count = 0;
    struct ibv_wc wc;
    struct ibv_qp_init_attr qp_attr;

    ret = init_resources(ctx, rai);
    if (ret) {
        printf("init_resources returned %llu\n", ret);
        return ret;
    }

    /* Use the srq_id as the listen_id since it is already setup */
    ctx->listen_id = ctx->srq_id;

    ret = rdma_listen(ctx->listen_id, 4);
    if (ret) {
        VERB_ERR("rdma_listen", ret);
        return ret;
    }
    printf("before, ctx->s_cb_pool[idx].request_buffer->tail is %llu\n", ctx->s_cb_pool[0].request_buffer->tail);              
    printf("waiting for connection from client..., qp nr is %d\n", ctx->qp_count);
    for (i = 0; i < ctx->qp_count; i++) {
        ret = rdma_get_request(ctx->listen_id, &ctx->conn_id[i]);
        if (ret) {
            VERB_ERR("rdma_get_request", ret);
            return ret;
        }

        /* Create the queue pair */
        memset(&qp_attr, 0, sizeof (qp_attr));

        qp_attr.qp_context = ctx;
        qp_attr.qp_type = IBV_QPT_RC;
        qp_attr.cap.max_send_wr = ctx->max_wr;
        qp_attr.cap.max_recv_wr = ctx->max_wr;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_sge = 1;
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
    printf("await_completion...\n");
    

    ret = await_completion(ctx);
    while (ibv_poll_cq(ctx->srq_cq, 1, &wc))
    {
        receive_mr(ctx, &wc);
        break;
    }  
    printf("after await_completion...\n");
    /*ret = await_completion(ctx);
    while (ibv_poll_cq(ctx->srq_cq, 1, &wc))
    {
        receive_head(ctx, &wc);
        break;
    }  */
    
    for (uint64_t i = 0; i < DEFAULT_THREADS; i++) {
        std::thread t(handle_RW, ctx, i);
        ctx->workers.push_back(move(t));
    }
    for (auto& worker : ctx->workers) {
        worker.join();
    }


    return 0;
}



/*
 * Function:    main
 * 
 * Input:
 *      argc    The number of arguments
 *      argv    Command line arguments
 * 
 * Output:
 *      none
 * 
 * Returns:
 *      0 on success, non-zero on failure
 * 
 * Description:
 *      Main program to demonstrate SRQ functionality.
 *      Both the client and server use an SRQ. ctx.qp_count number of QPs are
 *      created and each one of them uses the SRQ. After the connection, the
 *      client starts blasting sends to the server upto ctx.max_wr. When the
 *      server has received all the sends, it performs a send to the client to
 *      tell it that it can continue. Process repeats until ctx.msg_count
 *      sends have been performed.
 */
int main(uint64_t argc, char** argv)
{
    int ret, op;
    struct context ctx;
    struct rdma_addrinfo *rai, hints;

    memset(&ctx, 0, sizeof (ctx));
    memset(&hints, 0, sizeof (hints));

    ctx.server = 1;
    ctx.server_port = DEFAULT_PORT;
    ctx.msg_count = DEFAULT_MSG_COUNT;
    ctx.msg_length = DEFAULT_MSG_LENGTH;
    ctx.qp_count = DEFAULT_QP_COUNT;
    ctx.max_wr = DEFAULT_MAX_WR;

    /* Read options from command line */
    while ((op = getopt(argc, argv, "sa:p:c:l:q:w:")) != -1) {
        switch (op) {
        case 's':
            ctx.server = 1;
            break;
        case 'a':
            ctx.server_name = optarg;
            break;
        case 'p':
            ctx.server_port = optarg;
            break;
        case 'c':
            ctx.msg_count = atoi(optarg);
            break;
        case 'l':
            ctx.msg_length = atoi(optarg);
            break;
        case 'q':
            ctx.qp_count = atoi(optarg);
            break;
        case 'w':
            ctx.max_wr = atoi(optarg);
            break;
        default:
            printf("usage: %s -a server_address\n", argv[0]);
            printf("\t[-s server mode]\n");
            printf("\t[-p port_number]\n");
            printf("\t[-c msg_count]\n");
            printf("\t[-l msg_length]\n");
            printf("\t[-q qp_count]\n");
            printf("\t[-w max_wr]\n");
            exit(1);
        }
    }

    if (ctx.server_name == NULL) {
        printf("server address required (use -a)!\n");
        exit(1);
    }

    hints.ai_port_space = RDMA_PS_TCP;
    if (ctx.server == 1)
        hints.ai_flags = RAI_PASSIVE; /* this makes it a server */

    ret = rdma_getaddrinfo(ctx.server_name, ctx.server_port, &hints, &rai);
    if (ret) {
        VERB_ERR("rdma_getaddrinfo", ret);
        exit(1);
    }

    /* allocate memory for our QPs and send/recv buffers */
    ctx.conn_id = (struct rdma_cm_id **) calloc(ctx.qp_count,
                                                sizeof (struct rdma_cm_id *));
    memset(ctx.conn_id, 0, sizeof (ctx.conn_id));

    ctx.send_buf = (char *) malloc(ctx.msg_length);
    memset(ctx.send_buf, 0, ctx.msg_length);
    ctx.recv_buf = (char *) malloc(ctx.msg_length);
    memset(ctx.recv_buf, 0, ctx.msg_length);


    ctx.s_cb_pool = (struct ClientBuffer *)malloc(sizeof(struct ClientBuffer) * DEFAULT_QP_COUNT);
    ctx.s_sb_pool = (struct ServerBuffer *)malloc(sizeof(struct ServerBuffer) * DEFAULT_QP_COUNT);
    ctx.nr_process_read = (uint64_t *)malloc(DEFAULT_QP_COUNT * sizeof(uint64_t));
    ctx.nr_process_write = (uint64_t *)malloc(DEFAULT_QP_COUNT * sizeof(uint64_t));
    for (uint64_t i = 0; i < DEFAULT_QP_COUNT; i++) {
        ctx.s_cb_pool[i].request_buffer = (struct RemoteBuffer *)malloc(sizeof(struct RemoteBuffer));
        ctx.s_cb_pool[i].response_buffer = (struct RemoteBuffer *)malloc(sizeof(struct RemoteBuffer));
        ctx.s_cb_pool[i].request_buffer->buffer_mr = (struct ibv_mr *)malloc(sizeof(struct ibv_mr));
        ctx.s_cb_pool[i].response_buffer->buffer_mr = (struct ibv_mr *)malloc(sizeof(struct ibv_mr));
        ctx.s_cb_pool[i].request_buffer->tail = META_SIZE;
        ctx.s_cb_pool[i].response_buffer->head = META_SIZE;
        ctx.s_cb_pool[i].request_buffer->head = META_SIZE;
        ctx.s_cb_pool[i].response_buffer->tail = META_SIZE;
        ctx.s_cb_pool[i].request_buffer->same_page = true;
        ctx.s_cb_pool[i].response_buffer->same_page = true;
        ctx.s_sb_pool[i].recv_buffer = (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
        ctx.s_sb_pool[i].send_buffer = (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
        ctx.s_sb_pool[i].recv_buffer->data = (char *) malloc(BATCH_SIZE);
        ctx.s_sb_pool[i].send_buffer->data = (char *) malloc(WRITE_BUFFER_SIZE);
        ctx.s_sb_pool[i].recv_buffer->tail = 0;
        ctx.s_sb_pool[i].send_buffer->tail = 0;
        ctx.nr_process_write[i] = 0;
        ctx.nr_process_read[i] = 0;
        tasks.push(i);
    }
    

    ctx.kCapacity = 17179869184ull;
    ctx.kSegmentSize = 1073741824ull;
    ctx.num_segments = (uint64_t)(ctx.kCapacity / ctx.kSegmentSize);
    ctx.buffer_pool = (uint8_t **) malloc(sizeof(uint8_t*) * ctx.num_segments);
    for (uint64_t i = 0; i < ctx.num_segments; i++){
        ctx.buffer_pool[i] = (uint8_t *) malloc(sizeof(uint8_t) * ctx.kSegmentSize);
    } 
    ctx.write_flags = (bool *) malloc(sizeof(bool) * DEFAULT_QP_COUNT);
    ctx.write_across_metas = (struct write_across  *) malloc(sizeof(struct write_across) * DEFAULT_QP_COUNT);
    for (uint64_t i = 0; i < DEFAULT_QP_COUNT; i++) {
        ctx.write_flags[i] = false;
        ctx.write_across_metas[i].write_meta = (struct meta_data *) malloc(sizeof(struct meta_data));
        ctx.write_across_metas[i].tail = 0;
    }
    
    
    ret = run_server(&ctx, rai);
   

    destroy_resources(&ctx);
    free(rai);

    return ret;
}
