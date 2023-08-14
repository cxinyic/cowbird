/*
 * Compile Command:
 * g++ test_server.cc -o srq  -libverbs -lrdmacm -pthread
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

#define VERB_ERR(verb, ret) \
        fprintf(stderr, "%s returned %d errno %d\n", verb, ret, errno)

/* Default parameters values */
#define DEFAULT_PORT "51218"
#define DEFAULT_MSG_COUNT 10
#define DEFAULT_MSG_LENGTH 1048608
#define DEFAULT_QP_COUNT 16
#define DEFAULT_MAX_WR 64

#define BATCH_SIZE 4096
#define PAGE_SIZE 4096
#define COMPUTE_BUFFER_SIZE 1073741824+2*sizeof(int)
#define WRITE_SIZE 4096
#define WRITE_BUFFER_SIZE 314572800
#define TIMEOUT_IN_MS 500 /* ms */
#define META_SIZE 2*sizeof(int)

struct meta_data {
  int rw_type; // 0 for read; 1 for write
  uint64_t req_addr;
  uint64_t resp_addr;
  uint32_t length;
};

struct LocalBuffer {
  struct ibv_mr *buffer_mr;
  char *data;
};

struct ServerBuffer {
  struct LocalBuffer *recv_buffer;
  struct LocalBuffer *send_buffer;
};


/* Resources used in the example */
struct context
{
    /* User parameters */
    int server;
    char *server_name;
    char *server_port;
    int msg_count;
    int msg_length;
    int qp_count;
    int max_wr;

    /* Resources */
    struct ibv_pd *pd;
    struct rdma_cm_id *listen_id;
    struct rdma_cm_id **conn_id;
    struct ServerBuffer *s_sb_pool;
    struct ibv_comp_channel ** channels;
    struct ibv_cq ** cqs;

    std::vector<std::thread> workers;

    /* actual local buffer */
    uint64_t kSegmentSize;
    uint64_t kCapacity;
    int num_segments;
    uint8_t *buffer_pool;
    struct ibv_mr *server_buffer_mr;
    

    int* nr_process_read; 
    int* nr_process_write;
    
};


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
    int i;

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

    for (int i = 0; i < DEFAULT_QP_COUNT; i++) {
        if (ctx->s_sb_pool[i].recv_buffer->buffer_mr)
            rdma_dereg_mr(ctx->s_sb_pool[i].recv_buffer->buffer_mr);
        if (ctx->s_sb_pool[i].send_buffer->buffer_mr)
            rdma_dereg_mr(ctx->s_sb_pool[i].send_buffer->buffer_mr);
    }
}


int await_completion(struct context *ctx, int idx)
{
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
int run_server(struct context *ctx, struct rdma_addrinfo *rai)
{
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
 
    ret = rdma_bind_addr(ctx->listen_id, rai->ai_src_addr);
    if (ret) {
        VERB_ERR("rdma_bind_addr", ret);
        return ret;
    }   

    ret = rdma_listen(ctx->listen_id, 4);
    if (ret) {
        VERB_ERR("rdma_listen", ret);
        return ret;
    }

    printf("waiting for connection from client...\n");
    for (i = 0; i < ctx->qp_count; i++) {
        ret = rdma_get_request(ctx->listen_id, &ctx->conn_id[i]);
        if (ret) {
            VERB_ERR("rdma_get_request", ret);
            return ret;
        }
        printf("step1\n");
 
        ctx->channels[i] = ibv_create_comp_channel(ctx->conn_id[i]->verbs);
        if (!ctx->channels[i]) {
            VERB_ERR("ibv_create_comp_channel", -1);
            return -1;
        }
        printf("step2\n");
        ctx->cqs[i] = ibv_create_cq(ctx->conn_id[i]->verbs, ctx->max_wr, NULL,
                                ctx->channels[i], 0);
        if (!ctx->cqs[i]) {
            VERB_ERR("ibv_create_cq", -1);
            return -1;
        }    
        printf("step3\n");    
        ret = ibv_req_notify_cq(ctx->cqs[i], 0);
        if (ret) {
            VERB_ERR("ibv_req_notify_cq", ret);
            return ret;
        }
        printf("step4\n");

        /* Create the queue pair */
        memset(&qp_attr, 0, sizeof (qp_attr));

        qp_attr.qp_context = ctx;
        qp_attr.qp_type = IBV_QPT_RC;
        qp_attr.cap.max_send_wr = ctx->max_wr;
        qp_attr.cap.max_recv_wr = ctx->max_wr;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_sge = 1;
        qp_attr.cap.max_inline_data = 0;
        qp_attr.recv_cq = ctx->cqs[i];
        printf("step5\n");
        ret = rdma_create_qp(ctx->conn_id[i], NULL, &qp_attr);
        if (ret) {
            VERB_ERR("rdma_create_qp", ret);
            return ret;
        }
        printf("step6\n");
        
        ret = rdma_accept(ctx->conn_id[i], NULL);
        if (ret) {
            VERB_ERR("rdma_accept", ret);
            return ret;
        }
        
        
    }
    
    for (int i = 0; i < DEFAULT_QP_COUNT; i++) {
        ctx->s_sb_pool[i].recv_buffer->buffer_mr = rdma_reg_msgs(ctx->conn_id[i], ctx->s_sb_pool[i].recv_buffer->data, ctx->msg_length);
        if (!ctx->s_sb_pool[i].recv_buffer->buffer_mr) {
            VERB_ERR("rdma_reg_msgs", -1);
            return -1;
        }
        ctx->s_sb_pool[i].send_buffer->buffer_mr = rdma_reg_msgs(ctx->conn_id[i], ctx->s_sb_pool[i].send_buffer->data, ctx->msg_length);
        if (!ctx->s_sb_pool[i].send_buffer->buffer_mr) {
            VERB_ERR("rdma_reg_msgs", -1);
            return -1;
        }
    }
    printf("buffer pool is %p\n", ctx->buffer_pool);
    /*ctx->pd = ibv_alloc_pd(ctx->conn_id[0]->verbs);
    if (!ctx->pd){
        VERB_ERR("fail alloc pd", -1);
        return -1;
    }
    printf("after alloc pd\n");*/
    //ctx->server_buffer_mr = rdma_reg_msgs(ctx->conn_id[0], ctx->buffer_pool, ctx->kCapacity * sizeof(uint8_t));
    ctx->server_buffer_mr = ibv_reg_mr(ctx->conn_id[0]->pd, ctx->buffer_pool, 
    ctx->kCapacity * sizeof(uint8_t), 
    (IBV_ACCESS_LOCAL_WRITE |  IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC));
    if (!ctx->server_buffer_mr) {
        VERB_ERR("fail reg mr", -1);
        return -1;
    }
    printf("after reg mr\n");

    memcpy(ctx->s_sb_pool[0].send_buffer->data, ctx->server_buffer_mr, sizeof(struct ibv_mr));
    ret = rdma_post_send(ctx->conn_id[0], NULL, ctx->s_sb_pool[0].send_buffer->data, sizeof(struct ibv_mr), 
            ctx->s_sb_pool[0].send_buffer->buffer_mr, IBV_SEND_SIGNALED);
    ret = rdma_get_send_comp(ctx->conn_id[0], &wc);
    if (ret <= 0) {
        VERB_ERR("rdma_get_send_comp", ret);
        return -1;
    }
    printf("after send mr, addr is %llu\n", ctx->server_buffer_mr->addr);
    
    int *tmp = new int();
    *tmp= 0;
    while (1){
        
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
int main(int argc, char** argv)
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

    ctx.channels = (struct ibv_comp_channel**)calloc(ctx.qp_count,
                                                sizeof (struct ibv_comp_channel*));
    memset(ctx.channels, 0, sizeof (ctx.channels));
    ctx.cqs = (struct ibv_cq **)calloc(ctx.qp_count,
                                                sizeof (struct ibv_cq*));
    memset(ctx.cqs, 0, sizeof (ctx.cqs));

    ctx.server_buffer_mr = new struct ibv_mr();

    ctx.s_sb_pool = (struct ServerBuffer *)malloc(sizeof(struct ServerBuffer) * DEFAULT_QP_COUNT);
    ctx.nr_process_read = (int *)malloc(DEFAULT_QP_COUNT * sizeof(int));
    ctx.nr_process_write = (int *)malloc(DEFAULT_QP_COUNT * sizeof(int));
    for (int i = 0; i < DEFAULT_QP_COUNT; i++) {
        ctx.s_sb_pool[i].recv_buffer = (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
        ctx.s_sb_pool[i].send_buffer = (struct LocalBuffer *)malloc(sizeof(struct LocalBuffer));
        ctx.s_sb_pool[i].recv_buffer->data = (char *) malloc(ctx.msg_length);
        ctx.s_sb_pool[i].send_buffer->data = (char *) malloc(ctx.msg_length);
        ctx.nr_process_write[i] = 0;
        ctx.nr_process_read[i] = 0;
    }
    

    ctx.kCapacity = 17179869184ull;
    ctx.kSegmentSize = 1073741824ull;
    ctx.num_segments = (int)(ctx.kCapacity / ctx.kSegmentSize);
    ctx.buffer_pool = (uint8_t *) std::malloc(sizeof(uint8_t) *ctx.kCapacity);
    printf("buffer pool is %p\n", ctx.buffer_pool);
    /*ctx.buffer_pool = (uint8_t **) std::malloc(sizeof(uint8_t*) *ctx.num_segments);
    for (int i = 0; i < ctx.num_segments; i++){
        ctx.buffer_pool[i] = (uint8_t *) malloc(sizeof(uint8_t) * ctx.kSegmentSize);
    } */
    
    ret = run_server(&ctx, rai);
   

    destroy_resources(&ctx);
    free(rai);

    return ret;
}
