#include "libs/mgr.h"

#include <dlfcn.h> 
#include <pipe_mgr/pipe_mgr_intf.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h> 
#include <sys/ioctl.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>

#include <arpa/inet.h>

#include <iostream>

#define PKTGEN_SRC_PORT_PIPE0 68
#define ETHERTYPE_IPV4 0x0800
#define ETHERTYPE_VLAN 0x8100
#define UDP_TYPE 0x11

#define PD_DEV_PIPE_ALL 0xffff
#define PKTGEN_APP_1 0x1
#define PKTGEN_APP_2 0x1

#define MIN_PKTGEN_SIZE 54

#define DEV_ID 0

p4_pd_sess_hdl_t p4_sess_hdl;

typedef struct __attribute__((__packed__)) owseed_t {
  // Omit dst mac for optimization (6B auto-padding)
  uint8_t srcaddr[6];
  uint16_t type;
  char ipv4[20];
  char data[20]; 
} owseed;

owseed owseed_pkt;
uint8_t *upkt;
size_t sz = sizeof(owseed_pkt);

void init_seed(uint16_t ethertype, uint8_t prot) {
  uint8_t srcaddr[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  memcpy(owseed_pkt.srcaddr, srcaddr, 6);
  
  owseed_pkt.type = htons(ethertype);

  // 0-3b ver (4 for ipv4 to prevent bogus ipv4 ver)
  // 4-7b IHL (number of 32b words), min 5
  owseed_pkt.ipv4[0] = 0x45;
  owseed_pkt.ipv4[9] = prot;
 
  // Custom data
  uint8_t data[] = {0x00, 0x01, 0x01, 0x02, 0x03, 0x05, 0x08, 0x13, 0x21, 0x34, 0x55, 0x89};
  memcpy(owseed_pkt.data, data, 12);
  
  upkt = (uint8_t *) malloc(sz);
  memcpy(upkt, &owseed_pkt, sz);
}

void carve_ing_buffer(int pipeid) {

  int carved_cell_num = 1*1*64;

  bf_tm_ppg_hdl ppg;
  bf_tm_ppg_allocate(DEV_ID, 128*pipeid+68, &ppg);
  bf_tm_ppg_guaranteed_min_limit_set(DEV_ID, ppg, carved_cell_num);
  // IDLE is not admitted to TM upon violating the accounted usage on port basis
  bf_tm_port_ingress_drop_limit_set(DEV_ID, 128*pipeid+68, carved_cell_num);

}

void init_pktgen(int pipe_id, int app_id, int pkt_offset, int timer_ns, int batch_size) {
    
  int buffer_len = (sz < MIN_PKTGEN_SIZE)? MIN_PKTGEN_SIZE:sz;
  printf("buffer_len: %d\n", buffer_len);

  p4_pd_dev_target_t p4_pd_device;
  p4_pd_device.device_id = 0;
  p4_pd_device.dev_pipe_id = pipe_id;

  p4_pd_status_t pd_status;

  // Or full pkt but buffer_len-6, upkt+6
  pd_status = p4_pd_pktgen_write_pkt_buffer(sess_hdl, p4_pd_device, pkt_offset, buffer_len, upkt);
 
  if (pd_status != 0) {
    printf("Pktgen: Writing Packet buffer failed!\n");
    return;
  }
  p4_pd_complete_operations(sess_hdl);
    
  pd_status = p4_pd_pktgen_enable(sess_hdl, 0, PKTGEN_SRC_PORT_PIPE0+128*pipe_id);

  if (pd_status != 0) {
    printf("Failed to enable pktgen status = %d!!\n", pd_status);
    return;
  }

  struct p4_pd_pktgen_app_cfg prob_app_cfg;
        
  prob_app_cfg.trigger_type = PD_PKTGEN_TRIGGER_TIMER_PERIODIC;
  
  prob_app_cfg.batch_count = 0;
  prob_app_cfg.packets_per_batch = batch_size;
  prob_app_cfg.pattern_value = 0;
  prob_app_cfg.pattern_mask = 0;
  prob_app_cfg.timer_nanosec = timer_ns;
  prob_app_cfg.ibg = 0;
  prob_app_cfg.ibg_jitter = 0;
  prob_app_cfg.ipg = 0;
  prob_app_cfg.ipg_jitter = 0;
  prob_app_cfg.source_port = PKTGEN_SRC_PORT_PIPE0;//+128*pipe_id;
  prob_app_cfg.increment_source_port = 0;

  prob_app_cfg.pkt_buffer_offset = pkt_offset;
  prob_app_cfg.length = buffer_len;

  pd_status = p4_pd_pktgen_cfg_app(sess_hdl,
            p4_pd_device,
            app_id,
            prob_app_cfg);

  if (pd_status != 0) {
    printf("pktgen app configuration failed\n");
    return;
  }

  pd_status = p4_pd_pktgen_app_enable(sess_hdl, p4_pd_device, app_id);
  if (pd_status != 0) {
    printf("Pktgen : App enable Failed!\n");
    return;
  }
  printf("Launched pktgen for pipe %d\n", pipe_id);
//  sleep(1);
//  pd_status = p4_pd_pktgen_app_disable(sess_hdl, p4_pd_device, app_id);
//  if (pd_status != 0) {
//    printf("Pktgen : App disable Failed!\n");
//    return;
//  }
}

void set_egress_q_prio(int pipeid){
  int port_num  = 64;
  int q_count = 8;
  uint8_t q_mapping[8];
  int base_use_limit_cell_num = 1;
  int hysteresis = 0;
  int i;
  for(i = 0; i < q_count; ++i)
    q_mapping[i] = i;
  for(i = 0; i < port_num; ++i){
    bf_tm_port_q_mapping_set(DEV_ID, 128*pipeid+i, q_count, q_mapping);
    p4_pd_tm_set_q_sched_priority(DEV_ID, 128*pipeid+i, 0, BF_TM_SCH_PRIO_7);
    p4_pd_tm_set_q_sched_priority(DEV_ID, 128*pipeid+i, 1, BF_TM_SCH_PRIO_6);
    p4_pd_tm_set_q_sched_priority(DEV_ID, 128*pipeid+i, 2, BF_TM_SCH_PRIO_5);
    p4_pd_tm_set_q_sched_priority(DEV_ID, 128*pipeid+i, 3, BF_TM_SCH_PRIO_4);
    p4_pd_tm_set_q_sched_priority(DEV_ID, 128*pipeid+i, 4, BF_TM_SCH_PRIO_3);
    p4_pd_tm_set_q_sched_priority(DEV_ID, 128*pipeid+i, 5, BF_TM_SCH_PRIO_2);
    p4_pd_tm_set_q_sched_priority(DEV_ID, 128*pipeid+i, 6, BF_TM_SCH_PRIO_1);
    p4_pd_tm_set_q_sched_priority(DEV_ID, 128*pipeid+i, 7, BF_TM_SCH_PRIO_0);  // IDLE
    // Disable burst absorption factor for static limit of IDLE queue size (tail drop upon violation)
    bf_tm_q_app_pool_usage_set(DEV_ID, 128*pipeid+i, 7, BF_TM_EG_APP_POOL_3, base_use_limit_cell_num, BF_TM_Q_BAF_DISABLE, hysteresis);
  }
}

int main(int argc, char **argv) {

  orbweaver_config_t ow_config;
  start_switchd(argc, argv, &ow_config);

  printf("ethertype_seed: 0x%x\nprot_seed: 0x%x\ngap_seed: %d\n", 
         ow_config.ethertype_seed, ow_config.prot_seed, ow_config.gap_seed);
  printf("gen_seed: %s\n", ow_config.gen_seed ? "true" : "false");

  init_seed(ow_config.ethertype_seed, ow_config.prot_seed);

  set_egress_q_prio(0);
  carve_ing_buffer(0);

  sleep(10);

  if (ow_config.gen_seed) {
    init_pktgen(0, PKTGEN_APP_1, 0, ow_config.gap_seed, 0);
  }

  std::cout << "Joining switchd in main.cpp" << std::endl;
  return join_switchd();
}
