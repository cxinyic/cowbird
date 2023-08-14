#include <core.p4>
#include <tna.p4>

#define ETHERTYPE_IPV4 0x0800
#define IPV4_UDP 0x11

#define USER_PACKET 0x0
#define PKG_PACKET 0x1

#define MASK_1 0xff
#define MASK_2 0xffff

#define HIST_RANGE 131072
#define HIST_RANGE_POW2 17
#define NUM_RIGHT_SHIFT 0

#define EGR_RB_RANGE 131072
#define EGR_MONITORED_PORT 0x4

#define USER_TYPE 0x0
#define LOCAL_IDLE_TYPE 0x1
#define EXTERNAL_IDLE_TYPE 0x2
#define FILTER_WIDTH 16

#define OPCODE_WRITE_FIRST 0x06
#define OPCODE_WRITE_MIDDLE 0x07
#define OPCODE_WRITE_LAST 0x08
#define OPCODE_WRITE_ONLY 0x0a
#define OPCODE_READ_REQUEST 0x0c
#define OPCODE_READ_RESPONSE_FIRST 0x0d
#define OPCODE_READ_RESPONSE_MIDDLE 0x0e
#define OPCODE_READ_RESPONSE_LAST 0x0f
#define OPCODE_READ_RESPONSE_ONLY 0x10
#define OPCODE_ACK 0x11

#define PHASE_2_LEN 0x50
#define PHASE_2_LEN_RING_BUFFER 0x40

#define READ_REQUEST 0
#define WRITE_REQUEST 1

#define COMPUTE_IP 0x0a010101
#define MEMORY_IP 0x0a010105

#define CM_M 0x1
#define CM_CR 0x2
#define CM_CQ 0x3
#define CM_CR_ACK 0x4
#define CM_CQ_HEAD 0x5
#define CM_CQ_PKG 0x6

#define COMPUTE_METADATA_BUFFER_SIZE 0x3fffffff
#define NUM_THREADS 1







/*=============================================
=            Headers and metadata.            =
=============================================*/
typedef bit<48> mac_addr_t;
header ethernet_h {
    mac_addr_t dst_addr;
    mac_addr_t src_addr;
    bit<16> ether_type;
}

typedef bit<32> ipv4_addr_t;
header ipv4_h {
    bit<4> version;
    bit<4> ihl;
    bit<8> tos;
    bit<16> total_len;
    bit<16> identification;
    bit<3> flags;
    bit<13> frag_offset;
    bit<8> ttl;
    bit<8> protocol;
    bit<16> hdr_checksum;
    ipv4_addr_t src_addr;
    ipv4_addr_t dst_addr;
}

header udp_h {
    bit<16> src_port;
    bit<16> dst_port;
    bit<16> len;
    bit<16> checksum;
}

header bth_h {
    bit<8> opcode;
    bit<1> se;
    bit<1> mig_reg;
    bit<2> pad_count;
    bit<4> version;
    bit<16> p_key;
    bit<8> reserved_1;
    bit<24> dest_qp;
    bit<1> ack_req;
    bit<7> reserved_2;
    bit<24> psn;
}

header reth_h {
    bit<64> virtual_address;
    bit<32> r_key;
    bit<32> len;
}

header reth_split_h {
    bit<32> virtual_address_high;
    bit<32> virtual_address_low;
    bit<32> r_key;
    bit<32> len;
}

header aeth_h {
    bit<8> ack;
    bit<24> msn;
}

header phase_2_response_h {
    bit<8> rw_type;
    bit<8> padding1;
    bit<16> thread_id;
    bit<32> len;
    bit<32> req_addr_high;
    bit<32> req_addr_low;
    bit<32> resp_addr_high;
    bit<32> resp_addr_low;
    bit<64> padding;
}

header payload_h {
    bit<64> request_buffer_tail;
    bit<64> response_buffer_head;
}

header icrc_h {
    bit<32> crc;
}


header psn_offset_h {
    bit<32> psn_offset;
}
header tmp_h {
    bit<32> psn;
}

header cm_type_h {
    bit<8> cm;
}

header buffer_head_tail_h {
    bit<32> index;
}

header ring_buffer_head_h {
    bit<32> head;
    bit<32> thread_id;
    bit<64> padding;
}

header ring_buffer_head_tail_h {
    bit<32> head;
    bit<32> tail;
    bit<32> check;
    bit<32> exceed;
    bit<64> convert_tail;
    bit<32> same_page;
}

header multi_thread_h {
    bit<32> thread_id;
    bit<32> index_offset;
}


struct header_t {
    ethernet_h ethernet;
    ipv4_h ipv4;
    udp_h udp;
    bth_h bth;
    reth_h reth; 
    reth_split_h reth_split; 
    aeth_h aeth;
    phase_2_response_h phase_2_response;
    payload_h payload;
    ring_buffer_head_h ring_buffer_head;
    icrc_h icrc;
}

header ig_meas_h_ {
    bit<32> prev_ts_;
    bit<32> hash_;
    bit<32> diff_ts_ns_;
    bit<32> diff_ts_;
    bit<8> record_;
}

header ow_h {
    bit<8> type;
    bit<16> egr_port;
    bit<FILTER_WIDTH> mask;
    bit<FILTER_WIDTH> bitmap;
}

header eg_meas_h_ {
    bit<8> record_;
    bit<32> prev_ts_;
    bit<32> hash_;
    bit<32> diff_ts_;
    bit<32> port_local_seq_;
}

header response_len_log_h_ {
    bit<32> response_len_log;
    bit<32> response_len_tmp;
    bit<32> response_len_diff;
}

header phase_2_valid_h_ {
    bit<8> valid;
}

header counter_h_ {
    bit<8> ignore;
}

struct metadata_t {
    ig_meas_h_ hi_md_;
    eg_meas_h_ he_md_;
    ow_h ow_md;
    psn_offset_h psn_;
    tmp_h tmp;
    cm_type_h cm_type;
    buffer_head_tail_h buffer_head_tail;
    tmp_h expect_psn;
    ring_buffer_head_tail_h ring_buffer_head_tail;
    response_len_log_h_ response_len_log_h;
    multi_thread_h multi_thread;
    phase_2_valid_h_ phase_2_valid;
    counter_h_ counter;

}

/*===============================
=            Parsing            =
===============================*/
parser TofinoIngressParser(
        packet_in pkt,        
        out ingress_intrinsic_metadata_t ig_intr_md,
        out header_t hdr,
        out metadata_t md) {
    state start {
        pkt.extract(ig_intr_md);
        transition select(ig_intr_md.resubmit_flag) {
            1 : parse_resubmit;
            0 : parse_port_metadata;
        }
    }
    state parse_resubmit {
        transition reject;
    }
    state parse_port_metadata {
        pkt.advance(64); // skip this.
        transition accept;
    }
}

parser EthIpIngressParser(packet_in pkt, 
                   inout ingress_intrinsic_metadata_t ig_intr_md,
                   out header_t hdr,
		   out metadata_t md){

    ParserPriority() parser_prio;
    
    state start {
        transition select(ig_intr_md.ingress_port) {
	    196 : parse_seed;
	    default: parse_ethernet;
	}
    }
    state parse_seed {
        pkt.extract(hdr.ethernet);
        transition select(hdr.ethernet.ether_type) {
            ETHERTYPE_IPV4 : parse_ip;
            default : accept;
        }
    }
    state parse_ethernet {
        pkt.extract(hdr.ethernet);
        transition select(hdr.ethernet.ether_type) {
            ETHERTYPE_IPV4 : parse_ip;
            default : accept;
        }
    }
    state parse_ip {
        pkt.extract(hdr.ipv4);
        transition select(hdr.ipv4.protocol) {
            IPV4_UDP : parse_udp;
            default : accept;
        }
    }

    state parse_udp {
        pkt.extract(hdr.udp);
        transition parse_bth;
    }

    state parse_bth {
        pkt.extract(hdr.bth);
        transition select(hdr.bth.opcode) {
            OPCODE_READ_REQUEST : parse_reth;
            OPCODE_READ_RESPONSE_ONLY : parse_aeth;
            OPCODE_READ_RESPONSE_FIRST : parse_aeth;
            OPCODE_READ_RESPONSE_LAST : parse_aeth;
            OPCODE_ACK : parse_aeth;
            default : accept;
        }
    }

    state parse_reth {
        md.cm_type.cm = CM_CQ;
        pkt.extract(hdr.reth);
        transition select(hdr.bth.opcode) {
            OPCODE_READ_REQUEST : parse_icrc;
            default : accept;
        }
    }

    state parse_aeth {
        pkt.extract(hdr.aeth);
        transition select(hdr.bth.opcode) {
            OPCODE_READ_RESPONSE_ONLY : parse_read_response_only;
            default : accept;
        }

    }


    state parse_read_response_only {
        transition select(hdr.ipv4.total_len) {
            PHASE_2_LEN : parse_phase_2_response;
            PHASE_2_LEN_RING_BUFFER : parse_ring_buffer_head;
            default : accept;
        }
    }

    state parse_ring_buffer_head{
        pkt.extract(hdr.ring_buffer_head);
        transition parse_icrc;
    }

    state parse_phase_2_response{
        pkt.extract(hdr.phase_2_response);
        transition select(hdr.phase_2_response.rw_type) {
            READ_REQUEST : parse_phase_2_read;
            WRITE_REQUEST : parse_phase_2_write;
            default : accept;
        }
    }

    state parse_phase_2_read{
        md.cm_type.cm = CM_M;
        transition parse_icrc;
    }

    state parse_phase_2_write{
        md.cm_type.cm = CM_CQ;
        transition parse_icrc;
    }

    state parse_icrc {
        pkt.extract(hdr.icrc);
        transition accept;
    }
}

parser EthIpEgressParser(packet_in pkt, 
                   out header_t hdr,
		   out metadata_t md){

    state start {
        pkt.extract(hdr.ethernet);
        transition select(hdr.ethernet.ether_type) {
            ETHERTYPE_IPV4 : parse_ip;
            default : accept;
        }
    }
    state parse_ip {
        pkt.extract(hdr.ipv4);
        transition select(hdr.ipv4.protocol) {
            IPV4_UDP : parse_udp;
            default : accept;
        }
    }

    state parse_udp {
        pkt.extract(hdr.udp);
        transition parse_bth;
    }

    state parse_bth {
        pkt.extract(hdr.bth);
        transition select(hdr.bth.opcode) {
            OPCODE_READ_REQUEST : parse_reth;
            default : accept;
        }
    }

    state parse_reth {
        pkt.extract(hdr.reth);
        transition accept;
    }


}

parser TofinoEgressParser(
        packet_in pkt,
        out egress_intrinsic_metadata_t eg_intr_md) {
    state start {
        pkt.extract(eg_intr_md);
        transition accept;
    }
}

/*========================================
=            Ingress parsing             =
========================================*/
parser IngressParser(
        packet_in pkt,
        out header_t hdr, 
        out metadata_t md,
        out ingress_intrinsic_metadata_t ig_intr_md)
{
    state start {
        TofinoIngressParser.apply(pkt, ig_intr_md, hdr, md);
        EthIpIngressParser.apply(pkt, ig_intr_md, hdr, md);
        transition accept;
    }
}

/*===========================================
=            ingress match-action             =
===========================================*/
control Ingress(
        inout header_t hdr, 
        inout metadata_t md,
        in ingress_intrinsic_metadata_t ig_intr_md,
        in ingress_intrinsic_metadata_from_parser_t ig_prsr_md,
        inout ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md,
        inout ingress_intrinsic_metadata_for_tm_t ig_tm_md) {
    
    action ai_drop() {
        ig_dprsr_md.drop_ctl = 0x1;
        /*hdr.icrc.crc = 0x11112222;*/
    }
    action ai_nop() {
    }
    action ai_forward_user(bit<9> egress_port) {
        ig_tm_md.ucast_egress_port = egress_port;
    }
    action ai_forward_mirror_user_high_prio(bit<16> mc_gid) {
        ig_tm_md.mcast_grp_a = mc_gid;
        ig_tm_md.qid = 0x0;
    }
    action ai_forward_mirror_user_low_prio(bit<16> mc_gid) {
        ig_tm_md.mcast_grp_a = mc_gid;
        ig_tm_md.qid = 0x7;
    }
    action ai_drop_ti_forward_user() {
        ig_dprsr_md.drop_ctl = 0x1;
    }
    table ti_forward_user {
        key = {
	    hdr.ipv4.dst_addr : ternary;
        hdr.ipv4.src_addr : ternary;
	}
	actions = {
	    ai_forward_user;
	    ai_drop_ti_forward_user;
        ai_forward_mirror_user_high_prio;
        ai_forward_mirror_user_low_prio;
	}
    default_action = ai_forward_user(0x4);
    }
    /*===========================================
               craft eth: src/dst 
                     ip : src/dst
                     udp: src port
                     bth: qp_num/ack
               (for all)
    ===========================================*/


    action ai_craft_eth_ip_udp_bth(bit<48> eth_src_addr, bit<48> eth_dst_addr
    , bit<32> ip_src_addr, bit<32> ip_dst_addr, bit<16> udp_src_port, bit<24>
    qp_num, bit<1> ack) {
        hdr.ethernet.src_addr = eth_src_addr;
        hdr.ethernet.dst_addr = eth_dst_addr;
        hdr.ipv4.src_addr = ip_src_addr;
        hdr.ipv4.dst_addr = ip_dst_addr;
        hdr.udp.src_port = udp_src_port;
        hdr.bth.dest_qp = qp_num;
        hdr.bth.ack_req = ack;

    }

    table ti_craft_eth_ip_udp_bth{
        key = { 
        md.cm_type.cm : exact;
        md.multi_thread.thread_id : exact;
    }
    actions = {
        ai_craft_eth_ip_udp_bth;
        ai_nop;
    }
    default_action = ai_nop;
    }

    /*=========================================================
            Registers to store psn offsets
            craft psn offset(compute/memory each has one psn offset)
                  psn
            (for all)
            CM_CQ_PKG/CM_CQ_HEAD: index 0
            CM_CR_ACK: index 1
            CM_CR/CM_CQ: thread_id * 2 + 2
            CM_M: thread_id * 2 + 3
            we use two keys: cm type and thread id
    =========================================================*/

    Register<bit<32>, bit<32>>(64,0) ri_compute_psn_offset;
    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_compute_psn_offset) rai_compute_psn_offset = {
        void apply(inout bit<32> val, out bit<32> ret) {
            ret = val;      
            val = val + 1;            
        }
    };

    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_compute_psn_offset) rai_compute_psn_offset_phase_2 = {
        void apply(inout bit<32> val, out bit<32> ret) {
            ret = val;           
            val = val + md.response_len_log_h.response_len_log;                           
        }
    };

    action ai_drop_ti_compute_psn_offset() {
        ig_dprsr_md.drop_ctl = 0x1;
    }


    action ai_compute_psn_offset(bit<32> index) {
        md.psn_.psn_offset = rai_compute_psn_offset.execute(index);
    }
    table ti_compute_psn_offset {
        key = { 
        md.cm_type.cm : exact;
        md.multi_thread.thread_id : exact;
    }
        actions = {
            ai_compute_psn_offset;
            ai_drop_ti_compute_psn_offset;
    }
	default_action = ai_drop_ti_compute_psn_offset;
    }

    action ai_compute_psn_offset_phase_2(bit<32> index) {
        md.psn_.psn_offset = rai_compute_psn_offset_phase_2.execute(index);
    }
    table ti_compute_psn_offset_phase_2 {
        key = { 
        md.cm_type.cm : exact;
        md.multi_thread.thread_id : exact;
    }
        actions = {
            ai_compute_psn_offset_phase_2;
            ai_drop_ti_compute_psn_offset;
    }
	default_action = ai_drop_ti_compute_psn_offset;
    }

    action ai_drop_ti_craft_psn() {
        ig_dprsr_md.drop_ctl = 0x1;
    }


    action ai_craft_psn(bit<32> psn) {
        md.tmp.psn = psn + md.psn_.psn_offset;
    }
    table ti_craft_psn {
        key = { 
        md.cm_type.cm : exact;
        md.multi_thread.thread_id : exact;
    }
    actions = {
        ai_craft_psn;
        ai_drop_ti_craft_psn;
    }
    default_action = ai_drop_ti_craft_psn;
    }

    action ai_convert_psn() {
       hdr.bth.psn = (bit<24>)md.tmp.psn;
    }
    table ti_convert_psn {
      actions = {ai_convert_psn;}
      const default_action = ai_convert_psn();
    }

    /*=========================================================
               craft reth: va/rkey/len 
               (RDMA read request in phase 2/phase 3)
    =========================================================*/

    action ai_drop_ti_craft_reth() {
        ig_dprsr_md.drop_ctl = 0x1;
    }

    action ai_craft_reth(bit<64> virtual_address, bit<32> r_key, bit<32>
    len) {
        hdr.reth.virtual_address = virtual_address;
        hdr.reth.r_key = r_key;
        hdr.reth.len = len;
    }
    table ti_craft_reth {
        key = { 
        md.cm_type.cm : exact;
        md.multi_thread.thread_id : exact;
    }
    actions = {
        ai_craft_reth;
        ai_drop_ti_craft_reth;
    }
    default_action = ai_drop_ti_craft_reth;
    }

    action ai_craft_reth_phase_2() {
        hdr.reth_split.virtual_address_high = hdr.phase_2_response.req_addr_high;
        hdr.reth_split.virtual_address_low = hdr.phase_2_response.req_addr_low;
        hdr.reth_split.len = hdr.phase_2_response.len;
    }

    table ti_craft_reth_phase_2 {
        actions = {
            ai_craft_reth_phase_2;
            ai_nop;
    }
    default_action = ai_craft_reth_phase_2;
    }


    action ai_craft_rkey_phase_2(bit<32> r_key) {
        hdr.reth_split.r_key = r_key;
    }

    table ti_craft_rkey_phase_2 {
        key = {
        md.cm_type.cm : exact;
        md.multi_thread.thread_id : exact;
    }
    actions = {
        ai_craft_rkey_phase_2;
        ai_nop;
    }
    default_action = ai_nop;

    }
    
    /*=========================================================
               remove aeth; add reth;
               (for RDMA read response)
    =========================================================*/

    action ai_transfer_aeth_to_reth() {
      hdr.reth_split.setValid();
      hdr.aeth.setInvalid();
    }

    table ti_transfer_aeth_to_reth {
      actions = {ai_transfer_aeth_to_reth;}
      const default_action =ai_transfer_aeth_to_reth();
    }

    action ai_invalid_aeth() {
      hdr.aeth.setInvalid();
    }

    table ti_invalid_aeth {
      actions = {ai_invalid_aeth;}
      const default_action =ai_invalid_aeth();
    }

    /*=========================================================
               Registers to store va/len in phase 2 response
               ri_resp_addr_high: store high 32 bit addr
               ri_resp_addr_low: store low 32 bit addr
               ri_meta_data_tail : tail index
               ri_meta_data_head : head index
    =========================================================*/
    

    Register<bit<32>, bit<32>>(2048,0) ri_resp_addr_high;
    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_resp_addr_high) rai_update_resp_addr_high = {
        void apply(inout bit<32> val, out bit<32> ret) {
            ret = val;
            if (md.phase_2_valid.valid == 1)
            {
                val = hdr.phase_2_response.resp_addr_high;
            }
        }
    };

    action ai_update_resp_addr_high() {
        hdr.reth_split.virtual_address_high = rai_update_resp_addr_high.execute(md.buffer_head_tail.index);
    }

    table ti_update_resp_addr_high{
        actions = {
            ai_update_resp_addr_high;
        }
    default_action = ai_update_resp_addr_high;
    }

    Register<bit<32>, bit<32>>(2048,0) ri_resp_addr_low;
    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_resp_addr_low) rai_update_resp_addr_low = {
        void apply(inout bit<32> val, out bit<32> ret) {
            ret = val;
            if (md.phase_2_valid.valid == 1)
            {
                val = hdr.phase_2_response.resp_addr_low;
            }
        }
    };

    action ai_update_resp_addr_low() {  
        hdr.reth_split.virtual_address_low = rai_update_resp_addr_low.execute(md.buffer_head_tail.index);
        
    }

    table ti_update_resp_addr_low{
        actions = {
            ai_update_resp_addr_low;
        }
    default_action = ai_update_resp_addr_low;
    }

    Register<bit<32>, bit<32>>(2048,0) ri_resp_len;
    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_resp_len) rai_update_resp_len = {
        void apply(inout bit<32> val, out bit<32> ret) {
            ret = val;
            if (md.phase_2_valid.valid == 1)
            {
                val = hdr.phase_2_response.len;
            }
        }
    };
    

    action ai_update_resp_len() {
        hdr.reth_split.len = rai_update_resp_len.execute(md.buffer_head_tail.index);
    }

    table ti_update_resp_len{
        actions = {
            ai_update_resp_len;
        }
    default_action = ai_update_resp_len;
    }


    Register<bit<32>, bit<32>>(64,0) ri_meta_data_head;
    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_meta_data_head) rai_update_meta_data_head = {
        void apply(inout bit<32> val, out bit<32> ret) {
            ret = val;
            if (val == 127) {
                val = 0;
            } else {
                val = val + 1;
            }
        }
    };

    action ai_update_meta_data_head() {
        md.buffer_head_tail.index = rai_update_meta_data_head.execute(md.multi_thread.thread_id);
    }

    table ti_update_meta_data_head {
        actions = {
            ai_update_meta_data_head;
        }
    default_action = ai_update_meta_data_head;
    }

    Register<bit<32>, bit<32>>(64,0) ri_meta_data_tail;
    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_meta_data_tail) rai_update_meta_data_tail = {
        void apply(inout bit<32> val, out bit<32> ret) {
            ret = val;
            if (val == 127) {
                val = 0;
            } else {
                val = val + 1;
            }
        }
    };

    action ai_update_meta_data_tail() {
        md.buffer_head_tail.index = rai_update_meta_data_tail.execute(md.multi_thread.thread_id);
    }

    table ti_update_meta_data_tail {
        actions = {
            ai_update_meta_data_tail;
        }
    default_action = ai_update_meta_data_tail;
    }

    action ai_transfer_meta_data_index_by_thread_1() {
        md.multi_thread.index_offset = 128 * md.multi_thread.thread_id;
    }

    table ti_transfer_meta_data_index_by_thread_1 {
        actions = {
            ai_transfer_meta_data_index_by_thread_1;
        }
    default_action = ai_transfer_meta_data_index_by_thread_1;

    }

    action ai_transfer_meta_data_index_by_thread_2() {
        md.buffer_head_tail.index = md.buffer_head_tail.index + md.multi_thread.index_offset;
    }

    table ti_transfer_meta_data_index_by_thread_2 {
        actions = {
            ai_transfer_meta_data_index_by_thread_2;
        }
    default_action = ai_transfer_meta_data_index_by_thread_2;

    }

    /*=========================================================
              convert opcode
              read response xxx --> write xxx
    =========================================================*/


    action ai_convert_opcode(bit<8> opcode) {
        hdr.bth.opcode = opcode;
    }
    table ti_convert_opcode {
        key = { 
        hdr.bth.opcode : exact;
    }
    actions = {
        ai_convert_opcode;
        ai_nop;
    }
    default_action = ai_nop;
    }


    /*=========================================================
              CM_type: assign cm_type
    =========================================================*/

    action ai_cm_type(bit<8> type) {
        md.cm_type.cm = type;
    }

    @ignore_table_dependency("Ingress.ti_ack_cm_type")
    @ignore_table_dependency("Ingress.ti_transfer_aeth_to_reth_payload")
    table ti_cm_type{
        key = { 
        hdr.ipv4.src_addr : exact;
    }
        actions = {
            ai_cm_type;
    }
    }

    /*=========================================================
              craft opcode-specific header only
    =========================================================*/

    action ai_craft_header_phase_2() {
        hdr.ipv4.total_len = 60;
        hdr.udp.len = 40;       
        hdr.bth.pad_count = 0;
        hdr.bth.opcode = OPCODE_READ_REQUEST;
        hdr.phase_2_response.setInvalid();
    }

    table ti_craft_header_phase_2{
        actions = {
            ai_craft_header_phase_2;
    }
    default_action = ai_craft_header_phase_2;
    }

    action ai_craft_header_response_only() {
        hdr.ipv4.total_len = hdr.ipv4.total_len + 12;
        hdr.udp.len = hdr.udp.len + 12;       
    }

    table ti_craft_header_response_only{
        actions = {
            ai_craft_header_response_only;
    }
    default_action = ai_craft_header_response_only;
    }

    action ai_craft_header_response_first() {
        hdr.reth_split.len = 1024;
        hdr.bth.ack_req = 0;
    }

    table ti_craft_header_response_first{
        actions = {
            ai_craft_header_response_first;
    }
    default_action = ai_craft_header_response_first;
    }

    action ai_craft_header_response_middle() {
        hdr.bth.ack_req = 0;
    }

    table ti_craft_header_response_middle{
        actions = {
            ai_craft_header_response_middle;
    }
    default_action = ai_craft_header_response_middle;
    }

    action ai_craft_header_response_last() {
        hdr.ipv4.total_len = hdr.ipv4.total_len - 4;
        hdr.udp.len = hdr.udp.len - 4;
    }

    table ti_craft_header_response_last{
        actions = {
            ai_craft_header_response_last;
    }
    default_action = ai_craft_header_response_last;
    }

    action ai_craft_header_ack() {
        hdr.ipv4.total_len = hdr.ipv4.total_len + 28;
        hdr.udp.len = hdr.udp.len + 28;
    }

    table ti_craft_header_ack{
        actions = {
            ai_craft_header_ack;
    }
    default_action = ai_craft_header_ack;
    }



    /*=========================================================
              phase_3_ack only
    =========================================================*/


    action ai_ack_cm_type(bit<8> type) {
        md.cm_type.cm = type;
    }

    table ti_ack_cm_type {
        key = { 
        hdr.bth.dest_qp : exact;
        }
        actions = {
            ai_ack_cm_type;
            ai_nop;
        }
    default_action = ai_nop;
    }

    action ai_transfer_aeth_to_reth_payload() {
      hdr.reth.setValid();
      hdr.payload.setValid();
      hdr.aeth.setInvalid();
    }

    action ai_drop_ti_transfer_aeth_to_reth_payload() {
        ig_dprsr_md.drop_ctl = 0x1;
    }

    table ti_transfer_aeth_to_reth_payload {
      actions = {
        ai_transfer_aeth_to_reth_payload;
        ai_drop_ti_transfer_aeth_to_reth_payload();
      }
      const default_action =ai_transfer_aeth_to_reth_payload();
    }
    

    
    /*=========================================================
               compute node ring buffer management
    =========================================================*/
    Register<bit<32>, bit<32>>(64,0) ri_ring_buffer_head;
    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_ring_buffer_head) rai_ring_buffer_head = {
        void apply(inout bit<32> val, out bit<32> ret) {
            ret = val;
        }
    };


    action ai_ring_buffer_head() {
        md.ring_buffer_head_tail.head = rai_ring_buffer_head.execute(md.multi_thread.thread_id);
    }
    table ti_ring_buffer_head {
        actions = {
            ai_ring_buffer_head;
    }
	default_action = ai_ring_buffer_head;
    }

    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_ring_buffer_head) rai_ring_buffer_head_record = {
        void apply(inout bit<32> val, out bit<32> ret) {
            val = hdr.ring_buffer_head.head;
        }
    };


    action ai_ring_buffer_head_record() {
        rai_ring_buffer_head_record.execute((bit<32>)hdr.ring_buffer_head.thread_id);
    }
    table ti_ring_buffer_head_record {
        actions = {
            ai_ring_buffer_head_record;
    }
	default_action = ai_ring_buffer_head_record;
    }

    table ti_drop_after_record_head {
        actions = {
            ai_drop;
        }
    default_action = ai_drop;
    }

    Register<bit<32>, bit<32>>(64,0) ri_ring_buffer_same_page;
    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_ring_buffer_same_page) rai_ring_buffer_same_page = {
        void apply(inout bit<32> val, out bit<32> ret) {
            ret = val;
        }
    };
    action ai_ring_buffer_same_page() {
        md.ring_buffer_head_tail.same_page = rai_ring_buffer_same_page.execute(0);
    }
    table ti_ring_buffer_same_page {
        actions = {
            ai_ring_buffer_same_page;
    }
	default_action = ai_ring_buffer_same_page;
    }

    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_ring_buffer_same_page) rai_ring_buffer_same_page_record = {
        void apply(inout bit<32> val, out bit<32> ret) {
            val = md.ring_buffer_head_tail.same_page;
        }
    };

    action ai_ring_buffer_same_page_record() {
        rai_ring_buffer_same_page_record.execute(0);
    }
    table ti_ring_buffer_same_page_record {
        actions = {
            ai_ring_buffer_same_page_record;
    }
	default_action = ai_ring_buffer_same_page_record;
    }


    Register<bit<32>, bit<32>>(64,0) ri_ring_buffer_tail;
    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_ring_buffer_tail) rai_ring_buffer_tail = {
        void apply(inout bit<32> val, out bit<32> ret) {
            ret = val;
            /*if (val > md.ring_buffer_head_tail.head) {
                val = val + 32;
            }
            else {
                if (val + 32 <= md.ring_buffer_head_tail.head) {
                    val = val + 32;
                }
            }*/
            if (val > md.ring_buffer_head_tail.head || val + 32 <= md.ring_buffer_head_tail.head) {
                val = val + 32;
            }
        }
    };

    action ai_ring_buffer_tail() {
        md.ring_buffer_head_tail.tail = rai_ring_buffer_tail.execute(md.multi_thread.thread_id);
    }
    table ti_ring_buffer_tail {
        actions = {
            ai_ring_buffer_tail;
    }
	default_action = ai_ring_buffer_tail;
    }

    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_ring_buffer_tail) rai_ring_buffer_tail_ack = {
        void apply(inout bit<32> val, out bit<32> ret) {
            ret = val;
        }
    };

    action ai_get_request_buffer_tail() {
        hdr.payload.request_buffer_tail = (bit<64>)rai_ring_buffer_tail_ack.execute(md.multi_thread.thread_id);
    }
    table ti_get_request_buffer_tail {
        actions = {
            ai_get_request_buffer_tail;
    }
	default_action = ai_get_request_buffer_tail;
    }

    Register<bit<32>, bit<32>>(64,32) ri_response_buffer_head;
    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_response_buffer_head) rai_response_buffer_head = {
        void apply(inout bit<32> val, out bit<32> ret) {           
            ret = val;
            val = val + 32;
        }
    };

    action ai_get_response_buffer_head() {
        hdr.payload.response_buffer_head = (bit<64>)rai_response_buffer_head.execute(md.multi_thread.thread_id);
    }
    table ti_get_response_buffer_head {
        actions = {
            ai_get_response_buffer_head;
    }
	default_action = ai_get_response_buffer_head;
    }

    table ti_drop_if_overwhelm {
        key = {
            md.ring_buffer_head_tail.check : exact;
        }
        actions = {
            ai_drop;
            ai_nop;
        }
    default_action = ai_nop;
    }

    Register<bit<32>, bit<32>>(64,0) ri_ring_buffer_tail_copy;
    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_ring_buffer_tail_copy) rai_ring_buffer_tail_copy = {
        void apply(inout bit<32> val, out bit<32> ret) {           
            /*if (val > md.ring_buffer_head_tail.head) {
                val = val + 32;
                ret = 0;
            }
            else {
                if (val + 32 <= md.ring_buffer_head_tail.head) {
                    val = val + 32;
                    ret = 0;
                }
            }
            ret = 1;*/
            if (val > md.ring_buffer_head_tail.head || val + 32 <= md.ring_buffer_head_tail.head) {
                val = val + 32;
                ret = 0;
            } else {
                ret = 1;
            }
        }
    };
    


    action ai_ring_buffer_tail_copy() {
        md.ring_buffer_head_tail.exceed = rai_ring_buffer_tail_copy.execute(md.multi_thread.thread_id);
    }

    table ti_ring_buffer_tail_copy {
        actions = {
            ai_ring_buffer_tail_copy;
    }
	default_action = ai_ring_buffer_tail_copy;
    }

    action ai_assign_cm_type_read_request(bit<8> type) {
        md.cm_type.cm = type;
    }

    table ti_read_request_metadata_drop_if_exceed {
        key = {
            md.ring_buffer_head_tail.exceed : exact;
        }
        actions = {
            ai_drop;
            ai_nop;
        }
    default_action = ai_nop;
    }

    table ti_assign_cm_type_read_request {
        key = {
            hdr.reth.len : exact;
        }
        actions = {
            ai_assign_cm_type_read_request;
        }
    }

    action ai_get_real_tail() {
        md.ring_buffer_head_tail.tail = md.ring_buffer_head_tail.tail & COMPUTE_METADATA_BUFFER_SIZE;
    }


    table ti_get_real_tail {
        actions = {
            ai_get_real_tail;
        }
    default_action = ai_get_real_tail;
    }


    action ai_convert_reth_offset() {
        md.ring_buffer_head_tail.convert_tail = (bit<64>)md.ring_buffer_head_tail.tail;

    }

    table ti_convert_reth_offset {
        actions = {
            ai_convert_reth_offset;
    }
	default_action = ai_convert_reth_offset;

    }


    action ai_craft_reth_offset() {
        hdr.reth.virtual_address = hdr.reth.virtual_address +  md.ring_buffer_head_tail.convert_tail;
    }

    table ti_craft_reth_offset {
        actions = {
            ai_craft_reth_offset;
    }
	default_action = ai_craft_reth_offset;

    }

    action ai_convert_len_log_1() {
        md.response_len_log_h.response_len_log = hdr.phase_2_response.len >> 10;
    }
    table ti_convert_len_log_1 {
        actions = {
            ai_convert_len_log_1;
    }
	default_action = ai_convert_len_log_1;

    }


    action ai_convert_len_log_2() {
        md.response_len_log_h.response_len_tmp = hdr.phase_2_response.len & 0x3FF;     
    }
    table ti_convert_len_log_2 {
        actions = {
            ai_convert_len_log_2;
    }
	default_action = ai_convert_len_log_2;
    }

    action ai_convert_len_log_3() {
        md.response_len_log_h.response_len_log =
        md.response_len_log_h.response_len_log + 1;
    }

    table ti_convert_len_log_3 {
        key = {
            md.response_len_log_h.response_len_tmp: exact;
        }
        actions = {
            ai_convert_len_log_3;
            ai_nop();
        }
    default_action = ai_convert_len_log_3;
    }

    /*=========================================================
              multi thread related 
    =========================================================*/

    Register<bit<32>, bit<32>>(1,1) ri_thread_id_request;
    RegisterAction<bit<32>, bit<32>, bit<32>>(ri_thread_id_request) rai_thread_id_request = {
        void apply(inout bit<32> val, out bit<32> ret) {           
            ret = val;
            if (val == NUM_THREADS) {
                val = 1;
            } else {
                val = val + 1;
            }
        }
    };

    action ai_thread_id_request() {
        md.multi_thread.thread_id = rai_thread_id_request.execute(0);
    }

    table ti_thread_id_request {
        actions = {
            ai_thread_id_request;
        }
    default_action = ai_thread_id_request;
    }

    action ai_thread_id_phase_2() {
        md.multi_thread.thread_id = (bit<32>)hdr.phase_2_response.thread_id;
    }

    table ti_thread_id_phase_2 {
        actions = {
            ai_thread_id_phase_2;
        }
    default_action = ai_thread_id_phase_2;
    }

    action ai_phase_2_set_invalid() {
        md.phase_2_valid.valid = 0;
    }
    action ai_phase_2_set_valid() {
        md.phase_2_valid.valid = 1;
    }

    table ti_phase_2_response_check_valid {
        key = {
            hdr.bth.dest_qp : exact;
        }
        actions = {
            ai_phase_2_set_invalid;
            ai_phase_2_set_valid;
        }
    default_action = ai_phase_2_set_invalid;
    }

    action ai_thread_id_check_qp(bit<32> thread_id) {
        md.multi_thread.thread_id = thread_id;
    }

    table ti_thread_id_check_qp {
        key = {
            hdr.bth.dest_qp : exact;
        }
        actions = {
            ai_thread_id_check_qp;
            ai_nop;
        }
    default_action = ai_nop;
    }

    action ai_set_low_prio() {
        ig_tm_md.qid = 0x7;
    }

    table ti_set_low_prio {
        actions = {
            ai_set_low_prio;
        }
    default_action = ai_set_low_prio;
    }
    

    



    

    


    
    


    
    
    apply {
        /*=========================================================
                   check whether this is response in phase 2 
                   or phase 3
        =========================================================*/
        if (hdr.phase_2_response.isValid()) {
            ti_phase_2_response_check_valid.apply();
        }
        /*=========================================================
                   assign thread id 
        =========================================================*/
        
        /* round-robin; assign a thread id to read request */
        if (hdr.bth.opcode == OPCODE_READ_REQUEST) {
            ti_thread_id_request.apply();
            ti_assign_cm_type_read_request.apply();
            ti_ring_buffer_head.apply();
        }
        /* receive a response for head of ring buffer */
        /* the payload contains head + thread id */
        else if (hdr.ring_buffer_head.isValid()) {
            ti_ring_buffer_head_record.apply();
            ti_drop_after_record_head.apply();
        }
        /* receive a response for phase 2*/
        /* the payload contains the thread id */
        else if (md.phase_2_valid.valid == 1) {           
            ti_thread_id_phase_2.apply();
        }
        /* get the thread id by the queue pair number */
        else {
            ti_thread_id_check_qp.apply();
        }
        
        
        /*=================================================================================
                    assign cm type: to compute or memory; to request buffer or
                    response buffer
        =================================================================================*/
        if (hdr.bth.opcode == OPCODE_ACK) {
            ti_ack_cm_type.apply();
        }
        else if (md.phase_2_valid.valid == 0 && !hdr.ring_buffer_head.isValid() && (hdr.bth.opcode ==
            OPCODE_READ_RESPONSE_ONLY || hdr.bth.opcode ==
            OPCODE_READ_RESPONSE_FIRST || hdr.bth.opcode ==
            OPCODE_READ_RESPONSE_LAST || hdr.bth.opcode == OPCODE_READ_RESPONSE_MIDDLE
             )) {
            ti_cm_type.apply();
        }
        /*=================================================================================
                    get the gap for request PSN in phase 3 step 1
        =================================================================================*/
        else if (md.phase_2_valid.valid == 1) {
            ti_convert_len_log_1.apply();
            ti_convert_len_log_2.apply();
            ti_convert_len_log_3.apply();

        }  

        /*=================================================================================
                    record the metadata: resp addr + length
                    read the metadata: resp addr + length
        =================================================================================*/
        
        if ((hdr.bth.opcode == OPCODE_READ_RESPONSE_ONLY ||
        hdr.bth.opcode == OPCODE_READ_RESPONSE_FIRST ) &&( !hdr.ring_buffer_head.isValid())){
            ti_transfer_aeth_to_reth.apply();
            if (md.phase_2_valid.valid == 0 || hdr.bth.opcode == OPCODE_READ_RESPONSE_FIRST) {
                ti_update_meta_data_head.apply();
            } else {   
                ti_update_meta_data_tail.apply();
            } 
            ti_transfer_meta_data_index_by_thread_1.apply();
            ti_transfer_meta_data_index_by_thread_2.apply();
            
            if (hdr.bth.opcode == OPCODE_READ_RESPONSE_ONLY) {
                ti_update_resp_len.apply();
            }
            ti_update_resp_addr_low.apply();
            ti_update_resp_addr_high.apply();
        }
        /*=================================================================================
                    update the tail of the metadata request buffer and decide
                    whether we send read request to read head or metadata
        =================================================================================*/
        else if (hdr.bth.opcode == OPCODE_READ_REQUEST) {
            
            if (md.cm_type.cm == CM_CQ_PKG) {
                ti_ring_buffer_tail.apply();
                ti_ring_buffer_tail_copy.apply();
                ti_read_request_metadata_drop_if_exceed.apply();
                /*ti_get_response_buffer_head_check.apply();*/
            }
        }
        /*=================================================================================
                   get the current tail of the metadata request buffer 
                   and the current head of the finished requests
                   we will send these two values to the compute node 
        =================================================================================*/
        else if (md.cm_type.cm == CM_CR_ACK) {
            ti_transfer_aeth_to_reth_payload.apply();
            ti_get_request_buffer_tail.apply();
            ti_get_response_buffer_head.apply();
        }
        
        /*=================================================================================
                   craft reth for two kinds of packets:
                   we need two keys: cm_type and thread_id
                   1. read request in phase 2 step 1
                   2. write only in phase 4 step 1
        =================================================================================*/
        if (hdr.bth.opcode == OPCODE_READ_REQUEST || md.cm_type.cm ==
        CM_CR_ACK){
            ti_craft_reth.apply();
        }

        /*=================================================================================
                   craft common headers and the psns:
                   we need two keys: cm_type and thread_id
        =================================================================================*/
        
        if (ig_dprsr_md.drop_ctl != 0x1 && !hdr.ring_buffer_head.isValid() && (hdr.bth.opcode == OPCODE_READ_REQUEST ||hdr.bth.opcode ==
            OPCODE_READ_RESPONSE_ONLY || hdr.bth.opcode ==
            OPCODE_READ_RESPONSE_FIRST || hdr.bth.opcode ==
            OPCODE_READ_RESPONSE_LAST ||hdr.bth.opcode ==
            OPCODE_READ_RESPONSE_MIDDLE || md.cm_type.cm == CM_CR_ACK))
        {
            ti_craft_eth_ip_udp_bth.apply();
            if (md.phase_2_valid.valid == 1) {
                ti_compute_psn_offset_phase_2.apply();
            }
            else {
                ti_compute_psn_offset.apply();
            }
            ti_craft_psn.apply();
            ti_convert_psn.apply();   
        } 
        
        /*=================================================================================
                   craft rkey for some packets in phase 3 step 1/step 3
                   we know the address and length
                   we don't know the rkey
        =================================================================================*/
               
        if (hdr.bth.opcode != OPCODE_READ_REQUEST && hdr.bth.opcode !=
        OPCODE_READ_RESPONSE_MIDDLE && hdr.bth.opcode !=
        OPCODE_READ_RESPONSE_LAST && hdr.bth.opcode != OPCODE_ACK && !hdr.ring_buffer_head.isValid()){    
            ti_craft_rkey_phase_2.apply();           
        } 

        /*=================================================================================
                   since we transfer packets from type a type b
                   we need to change some fields in the common headers for every
                   type
                   for read request: we craft the virtual_address for RETH
                   for read response in phase 2: we craft the virtual address
                   and len for RETH
        =================================================================================*/
        
        if (hdr.bth.opcode == OPCODE_READ_RESPONSE_FIRST) {
            ti_craft_header_response_first.apply();
        }
        else if (hdr.bth.opcode == OPCODE_READ_RESPONSE_MIDDLE) {
            ti_craft_header_response_middle.apply();
        }
        else if (hdr.bth.opcode == OPCODE_READ_RESPONSE_LAST) {
            ti_invalid_aeth.apply();
            ti_craft_header_response_last.apply();
        }
        else if (hdr.bth.opcode == OPCODE_ACK) {
            if (md.cm_type.cm == CM_CR_ACK)
            { 
                ti_craft_header_ack.apply();
            }
        }    
        else if (hdr.bth.opcode == OPCODE_READ_REQUEST) {
            /*ti_set_low_prio.apply();*/
            if (md.cm_type.cm == CM_CQ_PKG) {
                /*ti_drop_if_overwhelm.apply();*/
                ti_get_real_tail.apply();
                ti_convert_reth_offset.apply();
                ti_craft_reth_offset.apply();
                
            } /*else if (md.cm_type.cm == CM_CQ_HEAD){
                ti_set_low_prio.apply();
            } */     
        }
        else if (hdr.bth.opcode == OPCODE_READ_RESPONSE_ONLY) {
            if (md.phase_2_valid.valid == 1) {
                ti_craft_reth_phase_2.apply();
                ti_craft_header_phase_2.apply();
            } else {
                ti_craft_header_response_only.apply();
            }
        }
        
        
        if (hdr.bth.opcode == OPCODE_READ_RESPONSE_ONLY || hdr.bth.opcode ==
        OPCODE_READ_RESPONSE_FIRST || hdr.bth.opcode ==
        OPCODE_READ_RESPONSE_LAST || hdr.bth.opcode ==
        OPCODE_READ_RESPONSE_MIDDLE || md.cm_type.cm == CM_CR_ACK
        ){
            ti_convert_opcode.apply();
        }
        ti_forward_user.apply();
        
        
    }

    
}

control IngressDeparser(
        packet_out pkt, 
        inout header_t hdr, 
        in metadata_t md,
        in ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md) {
    Checksum() ipv4_csum;
    

    apply {
        
        if(hdr.ipv4.isValid()) {
            hdr.ipv4.hdr_checksum = ipv4_csum.update({
                hdr.ipv4.version,
                hdr.ipv4.ihl,
                hdr.ipv4.tos,
                hdr.ipv4.total_len,
                hdr.ipv4.identification,
                hdr.ipv4.flags,
                hdr.ipv4.frag_offset,
                hdr.ipv4.ttl,
                hdr.ipv4.protocol,
                hdr.ipv4.src_addr,
                hdr.ipv4.dst_addr });
        }
        
        
        pkt.emit(hdr);
    }
}

/*======================================
=            Egress parsing            =
======================================*/
parser EgressParser(
        packet_in pkt,
        out header_t hdr, 
        out metadata_t eg_md,
        out egress_intrinsic_metadata_t eg_intr_md) {
    TofinoEgressParser() tofino_parser;
    EthIpEgressParser() eth_ip_parser; 
    state start {
        tofino_parser.apply(pkt, eg_intr_md);
        transition parse_packet;
    }
    state parse_packet {
        eth_ip_parser.apply(pkt, hdr, eg_md);
        transition accept;        
    }
}

/*=========================================
=            Egress match-action            =
=========================================*/

control Egress(
        inout header_t hdr, 
        inout metadata_t eg_md,
        in egress_intrinsic_metadata_t eg_intr_md,
        in egress_intrinsic_metadata_from_parser_t eg_prsr_md,
        inout egress_intrinsic_metadata_for_deparser_t eg_dprsr_md,
        inout egress_intrinsic_metadata_for_output_port_t eg_oport_md){

    Register<bit<32>, bit<32>>(256, 0) re_port2ctr_;
    RegisterAction<bit<32>, bit<32>, bit<32>>(re_port2ctr_) rae_port2ctr_ = {
        void apply(inout bit<32> val, out bit<32> ret) {
            ret = val; 
            val = val + 1;
        }
    };
    action ae_port2ctr_() {
        rae_port2ctr_.execute((bit<32>)eg_intr_md.egress_port);
    }
    table te_port2ctr_ {
        actions = { ae_port2ctr_; }
    const default_action = ae_port2ctr_();
    }

    Register<bit<32>, bit<32>>(256, 0) re_port2ctr_byte;
    RegisterAction<bit<32>, bit<32>, bit<32>>(re_port2ctr_byte) rae_port2ctr_byte = {
        void apply(inout bit<32> val, out bit<32> ret) {
            ret = val; 
            val = val + (bit<32>)hdr.ipv4.total_len;
        }
    };
    action ae_port2ctr_byte() {
        rae_port2ctr_byte.execute((bit<32>)eg_intr_md.egress_port);
    }
    table te_port2ctr_byte {
        actions = { ae_port2ctr_byte; }
    const default_action = ae_port2ctr_byte();
    }

    action ai_counter_ignore_check() {
        eg_md.counter.ignore = 1;
    }

    action ai_nop() {
    }

    table ti_counter_ignore_check {
        key = {
            hdr.bth.dest_qp : exact;
        }
        actions = {
            ai_counter_ignore_check;
            ai_nop;
        }
    const default_action = ai_nop;
    }
    

    apply { 
        
    }
}

control EgressDeparser(
        packet_out pkt,
        inout header_t hdr, 
        in metadata_t eg_md,
        in egress_intrinsic_metadata_for_deparser_t eg_dprsr_md) {
    apply {
        pkt.emit(hdr);
    }
}

/*==============================================
=            The switch's pipeline             =
==============================================*/
Pipeline(
    IngressParser(), Ingress(), IngressDeparser(),
    EgressParser(), Egress(), EgressDeparser()) pipe;

Switch(pipe) main;