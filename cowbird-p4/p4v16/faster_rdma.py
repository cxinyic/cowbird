import argparse
import json
import sys, os, time

sys.path.append(os.path.dirname(os.path.realpath(__file__))+"/libs")
from mgr import *





def config():
    print("PID: " + str(os.getpid()))
    m = Manager(p4name="cowbird_p4")

    mc_pipe0_ports = [0 << 7 | i for i in range(0, 61, 4)]
    mc_pipe1_ports = [1 << 7 | i for i in range(0, 61, 4)]

    data = {}
    with open('libs/servers.json', 'r') as f:
        servers_json = json.loads(f.read())
    server_ports = [155]
#    for val in servers_json.values():
#        server_ports.append(val['port_id'])

    # 1/0 <-> 2/0
    #loopback_ports = [132, 140, 155]
    compute_memory_ports = [180, 188]

    compute_server_ether_addr = 0x1c34da6a3056
    memory_server_ether_addr = 0x1c34da6a2f52
    compute_server_ip_addr = 0x0a010109
    memory_server_ip_addr = 0x0a010108
    meta_size = 16
    OPCODE_WRITE_FIRST = 0x06
    OPCODE_WRITE_MIDDLE = 0x07
    OPCODE_WRITE_LAST = 0x08
    OPCODE_WRITE_ONLY = 0x0a
    OPCODE_READ_REQUEST = 0x0c
    OPCODE_READ_RESPONSE_FIRST = 0x0d
    OPCODE_READ_RESPONSE_MIDDLE = 0x0e
    OPCODE_READ_RESPONSE_LAST = 0x0f
    OPCODE_READ_RESPONSE_ONLY = 0x10
    OPCODE_ACK = 0x11

    
    CM_M = 0x1
    CM_CR = 0x2
    CM_CQ = 0x3
    CM_CR_ACK = 0x4
    CM_CQ_HEAD = 0x5
    CM_CQ_PKG = 0x6
    NUM_THREAD = 2

    

    print("--- Set up ports (physically occupied for successful enabling) ---")
#    m.enab_ports_sym_bw(mc_pipe0_ports, "100G")
    m.enab_ports_sym_bw(server_ports, "25G")
    m.enab_ports_sym_bw_rs(compute_memory_ports, "100G")

    print("--- Create MC groups (binning 4 ports as an example) ---")
    mc_pipe0_combinations = [
        [188, 192],
        [180, 192],
        [155, 192],
    ]

    mc_grp_ids = [i for i in range(201, 204)]
    for i in range(len(mc_grp_ids)):
        m.create_mc_grp(mc_grp_ids[i], mc_pipe0_combinations[i])

    print("--- Configure DP states ---")
    '''for server in servers_json.values():
        m.add_rule_ternary_read_action_arg(tbl_name="ti_forward_user",
                                           action_name='ai_forward_user',
                                           match_arg_name="hdr.ipv4.dst_addr",
                                           match_arg_ternary_annotation="ipv4",
                                           match_arg_ternary=server['ip_addr'],
                                           match_arg_ternary_mask="255.255.255.255",
                                           priority=0x2,
                                           action_arg_name="egress_port",
                                           action_arg=server['port_id'])'''
    '''m.add_rule_ternary_read_action_arg(tbl_name="ti_forward_user",
                                           action_name='ai_forward_mirror_user',
                                           match_arg_name="hdr.ipv4.dst_addr",
                                           match_arg_ternary_annotation="ipv4",
                                           match_arg_ternary=0x0a010101,
                                           match_arg_ternary_mask="255.255.255.255",
                                           priority=0x2,
                                           action_arg_name="mc_gid",
                                           action_arg=201)
    m.add_rule_ternary_read_action_arg(tbl_name="ti_forward_user",
                                           action_name='ai_forward_mirror_user',
                                           match_arg_name="hdr.ipv4.dst_addr",
                                           match_arg_ternary_annotation="ipv4",
                                           match_arg_ternary=0x0a01010f,
                                           match_arg_ternary_mask="255.255.255.255",
                                           priority=0x2,
                                           action_arg_name="mc_gid",
                                           action_arg=202)
    m.add_rule_ternary_read_action_arg(tbl_name="ti_forward_user",
                                           action_name='ai_forward_mirror_user',
                                           match_arg_name="hdr.ipv4.dst_addr",
                                           match_arg_ternary_annotation="ipv4",
                                           match_arg_ternary=0x0a010103,
                                           match_arg_ternary_mask="255.255.255.255",
                                           priority=0x2,
                                           action_arg_name="mc_gid",
                                           action_arg=203)'''

    m.add_rule_ternary_read2_action_arg(tbl_name="ti_forward_user",
                                           action_name='ai_forward_mirror_user_high_prio',
                                           match_arg_name0="hdr.ipv4.dst_addr",
                                           match_arg_ternary_annotation="ipv4",
                                           match_arg_ternary0=0x0a010109,
                                           match_arg_ternary_mask0="255.255.255.255",
                                           match_arg_name1="hdr.ipv4.src_addr",
                                           match_arg_ternary1=0x0a010108,
                                           match_arg_ternary_mask1="255.255.255.255",
                                           priority=0x2,
                                           action_arg_name="mc_gid",
                                           action_arg=201)

    m.add_rule_ternary_read2_action_arg(tbl_name="ti_forward_user",
                                           action_name='ai_forward_mirror_user_low_prio',
                                           match_arg_name0="hdr.ipv4.dst_addr",
                                           match_arg_ternary_annotation="ipv4",
                                           match_arg_ternary0=0x0a010109,
                                           match_arg_ternary_mask0="255.255.255.255",
                                           match_arg_name1="hdr.ipv4.src_addr",
                                           match_arg_ternary1=0x0a010103,
                                           match_arg_ternary_mask1="255.255.255.255",
                                           priority=0x2,
                                           action_arg_name="mc_gid",
                                           action_arg=201)
    
    m.add_rule_ternary_read2_action_arg(tbl_name="ti_forward_user",
                                           action_name='ai_forward_mirror_user_low_prio',
                                           match_arg_name0="hdr.ipv4.dst_addr",
                                           match_arg_ternary_annotation="ipv4",
                                           match_arg_ternary0=0x0a010103,
                                           match_arg_ternary_mask0="255.255.255.255",
                                           match_arg_name1="hdr.ipv4.src_addr",
                                           match_arg_ternary1=0x0a010109,
                                           match_arg_ternary_mask1="255.255.255.255",
                                           priority=0x2,
                                           action_arg_name="mc_gid",
                                           action_arg=203)

    m.add_rule_ternary_read2_action_arg(tbl_name="ti_forward_user",
                                           action_name='ai_forward_mirror_user_high_prio',
                                           match_arg_name0="hdr.ipv4.dst_addr",
                                           match_arg_ternary_annotation="ipv4",
                                           match_arg_ternary0=0x0a010108,
                                           match_arg_ternary_mask0="255.255.255.255",
                                           match_arg_name1="hdr.ipv4.src_addr",
                                           match_arg_ternary1=0x0a010109,
                                           match_arg_ternary_mask1="255.255.255.255",
                                           priority=0x2,
                                           action_arg_name="mc_gid",
                                           action_arg=202)
    
    print("--- waiting to get RDMA parameters ---")
    #print("pipe0&1 re_port2ctr_")
    #time.sleep(130)
    '''for port in range(256):
        value = m.read_reg_element_for_pipe("re_port2ctr_", port, pipeid=port_to_pipe(port))
        if value != 0:
            print(port, value)

    for port in range(256):
        value = m.read_reg_element_for_pipe("re_port2ctr_byte", port, pipeid=port_to_pipe(port))
        if value != 0:
            print(port, value)
    for port in range(256):
        value = m.read_reg_element_for_pipe("re_port2ctr_ignore", port, pipeid=port_to_pipe(port))
        if value != 0:
            print(port, value)'''
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    ip_port = ('158.130.4.216', 8887)
    sock.bind(ip_port)
    sock.listen(5)
    conn, address = sock.accept()
    compute_qp = []
    memory_qp = []
    compute_psn = []
    memory_psn = []
    compute_udp_src_port = []
    memory_udp_src_port = []
    compute_meta_req_addr = []
    compute_meta_req_rkey = []
    compute_meta_resp_addr = []
    compute_meta_resp_rkey = []
    compute_data_req_rkey = []
    compute_data_resp_rkey = []

    for id in range(NUM_THREAD):
        for i in range(12):
            data = conn.recv(16)
            print(id, i+1, data)
            x = int(data.strip(b'\x00'.decode()), 16)
            if i == 0:
                compute_qp.append(x)
            if i == 1:
                memory_qp.append(x)
            if i == 2:
                compute_psn.append(x)
            if i == 3:
                memory_psn.append(x)
            if i == 4:
                compute_udp_src_port.append(x)
            if i == 5:
                memory_udp_src_port.append(x)
            if i == 6:
                compute_meta_req_addr.append(x)
            if i == 7:
                compute_meta_req_rkey.append(x)
            if i == 8:
                compute_meta_resp_addr.append(x)
            if i == 9:
                compute_meta_resp_rkey.append(x)
            if i == 10:
                compute_data_req_rkey.append(x)
            if i == 11:
                compute_data_resp_rkey.append(x)
    
    for i in range(6):
        data = conn.recv(16)
        print(i+1, data)
        x = int(data.strip(b'\x00'.decode()), 16)
        if i == 0:
            compute_qp_p4_req = x
        if i == 1:
            memory_qp_p4_req = x
        if i == 2:
            compute_psn_p4_req = x
        if i == 3:
            memory_psn_p4_req = x
        if i == 4:
            compute_udp_src_port_p4_req = x
        if i == 5:
            memory_udp_src_port_p4_req = x

    for i in range(6):
        data = conn.recv(16)
        print(i+1, data)
        x = int(data.strip(b'\x00'.decode()), 16)
        if i == 0:
            compute_qp_p4_req_2 = x
        if i == 1:
            memory_qp_p4_req_2 = x
        if i == 2:
            compute_psn_p4_req_2 = x
        if i == 3:
            memory_psn_p4_req_2 = x
        if i == 4:
            compute_udp_src_port_p4_req_2 = x
        if i == 5:
            memory_udp_src_port_p4_req_2 = x

    for i in range(6):
        data = conn.recv(16)
        print(i+1, data)
        x = int(data.strip(b'\x00'.decode()), 16)       
        if i == 0:
            compute_qp_p4_head = x
        if i == 1:
            memory_qp_p4_head = x
        if i == 2:
            compute_psn_p4_head = x
        if i == 3:
            memory_psn_p4_head = x
        if i == 4:
            compute_udp_src_port_p4_head = x
        if i == 5:
            memory_udp_src_port_p4_head = x

    for i in range(6):
        data = conn.recv(16)
        print(i+1, data)
        x = int(data.strip(b'\x00'.decode()), 16)       
        if i == 0:
            compute_qp_p4_finish = x
        if i == 1:
            memory_qp_p4_finish = x
        if i == 2:
            compute_psn_p4_finish = x
        if i == 3:
            memory_psn_p4_finish = x
        if i == 4:
            compute_udp_src_port_p4_finish = x
        if i == 5:
            memory_udp_src_port_p4_finish = x

    for i in range(2):
        data = conn.recv(16)
        print(i+1, data)
        x = int(data.strip(b'\x00'.decode()), 16)       
        if i == 0:
            memory_pool_addr = x
        if i == 1:
            memory_pool_rkey = x

    m.add_rule_exact_read(tbl_name="ti_phase_2_response_check_valid",
                                     action_name='ai_phase_2_set_valid',
                                     match_arg_exact_name="hdr.bth.dest_qp",
                                     match_arg_exact=memory_qp_p4_req)
    m.add_rule_exact_read(tbl_name="ti_phase_2_response_check_valid",
                                     action_name='ai_phase_2_set_valid',
                                     match_arg_exact_name="hdr.bth.dest_qp",
                                     match_arg_exact=memory_qp_p4_req_2)
    m.add_rule_exact_read(tbl_name="ti_read_request_metadata_drop_if_exceed",
                                     action_name='ai_drop',
                                     match_arg_exact_name="md.ring_buffer_head_tail.exceed",
                                     match_arg_exact=1)
    
    m.add_rule_exact_read_action_arg(tbl_name="ti_assign_cm_type_read_request",
                                     action_name='ai_assign_cm_type_read_request',
                                     match_arg_name0="hdr.reth.len",
                                     match_arg0=32,
                                     action_arg_name="type",
                                     action_arg=CM_CQ_PKG)
    
    m.add_rule_exact_read_action_arg(tbl_name="ti_assign_cm_type_read_request",
                                     action_name='ai_assign_cm_type_read_request',
                                     match_arg_name0="hdr.reth.len",
                                     match_arg0=16,
                                     action_arg_name="type",
                                     action_arg=CM_CQ_HEAD)

    m.add_rule_exact_read_action_arg(tbl_name="ti_convert_opcode",
                                     action_name='ai_convert_opcode',
                                     match_arg_name0="hdr.bth.opcode",
                                     match_arg0=OPCODE_READ_RESPONSE_ONLY,
                                     action_arg_name="opcode",
                                     action_arg=OPCODE_WRITE_ONLY)
    
    m.add_rule_exact_read_action_arg(tbl_name="ti_convert_opcode",
                                     action_name='ai_convert_opcode',
                                     match_arg_name0="hdr.bth.opcode",
                                     match_arg0=OPCODE_READ_RESPONSE_FIRST,
                                     action_arg_name="opcode",
                                     action_arg=OPCODE_WRITE_FIRST)
    
    m.add_rule_exact_read_action_arg(tbl_name="ti_convert_opcode",
                                     action_name='ai_convert_opcode',
                                     match_arg_name0="hdr.bth.opcode",
                                     match_arg0=OPCODE_READ_RESPONSE_LAST,
                                     action_arg_name="opcode",
                                     action_arg=OPCODE_WRITE_LAST)
    
    m.add_rule_exact_read_action_arg(tbl_name="ti_convert_opcode",
                                     action_name='ai_convert_opcode',
                                     match_arg_name0="hdr.bth.opcode",
                                     match_arg0=OPCODE_READ_RESPONSE_MIDDLE,
                                     action_arg_name="opcode",
                                     action_arg=OPCODE_WRITE_MIDDLE)

    m.add_rule_exact_read_action_arg(tbl_name="ti_convert_opcode",
                                     action_name='ai_convert_opcode',
                                     match_arg_name0="hdr.bth.opcode",
                                     match_arg0=OPCODE_ACK,
                                     action_arg_name="opcode",
                                     action_arg=OPCODE_WRITE_ONLY)
            
    m.add_rule_exact_read_action_arg(tbl_name="ti_cm_type",
                                     action_name='ai_cm_type',
                                     match_arg_name0="hdr.ipv4.src_addr",
                                     match_arg0=compute_server_ip_addr,
                                     action_arg_name="type",
                                     action_arg=CM_M)

    m.add_rule_exact_read_action_arg(tbl_name="ti_cm_type",
                                     action_name='ai_cm_type',
                                     match_arg_name0="hdr.ipv4.src_addr",
                                     match_arg0=memory_server_ip_addr,
                                     action_arg_name="type",
                                     action_arg=CM_CR)

    m.add_rule_exact_read(tbl_name="ti_convert_len_log_3",
                                     action_name='ai_nop',
                                     match_arg_exact_name="md.response_len_log_h.response_len_tmp",
                                     match_arg_exact=0)

    '''m.add_rule_exact_read(tbl_name="ti_counter_ignore_check",
                                     action_name='ai_counter_ignore_check',
                                     match_arg_exact_name="hdr.bth.dest_qp",
                                     match_arg_exact=compute_qp_p4_head)
    m.add_rule_exact_read(tbl_name="ti_counter_ignore_check",
                                     action_name='ai_counter_ignore_check',
                                     match_arg_exact_name="hdr.bth.dest_qp",
                                     match_arg_exact=memory_qp_p4_head)
                                     
    m.add_rule_exact_read(tbl_name="ti_drop_if_overwhelm",
                                     action_name='ai_drop',
                                     match_arg_exact_name="md.ring_buffer_head_tail.check",
                                     match_arg_exact=1)'''
    # thread's related rules
    seen = []
    for idx in range(NUM_THREAD):
        print(idx, "add rule")
        
        m.add_rule_exact_read_action_arg(tbl_name="ti_thread_id_check_qp",
                                        action_name='ai_thread_id_check_qp',
                                        match_arg_name0="hdr.bth.dest_qp",
                                        match_arg0=compute_qp[idx],
                                        action_arg_name="thread_id",
                                        action_arg=idx+1)
        m.add_rule_exact_read_action_arg(tbl_name="ti_ack_cm_type",
                                        action_name='ai_ack_cm_type',
                                        match_arg_name0="hdr.bth.dest_qp",
                                        match_arg0=compute_qp[idx],
                                        action_arg_name="type",
                                        action_arg=CM_CR_ACK)
        
        m.add_rule_exact_read_action_arg(tbl_name="ti_thread_id_check_qp",
                                        action_name='ai_thread_id_check_qp',
                                        match_arg_name0="hdr.bth.dest_qp",
                                        match_arg0=memory_qp[idx],
                                        action_arg_name="thread_id",
                                        action_arg=idx+1)
        
        m.add_rule_exact_read_action_arg(tbl_name="ti_ack_cm_type",
                                        action_name='ai_ack_cm_type',
                                        match_arg_name0="hdr.bth.dest_qp",
                                        match_arg0=memory_qp[idx],
                                        action_arg_name="type",
                                        action_arg=CM_CR_ACK)

        m.add_rule_exact_read2_action_arg3(tbl_name="ti_craft_reth",
                                      action_name='ai_craft_reth',
                                      match_arg_name0="md.cm_type.cm",
                                      match_arg0=CM_CQ_PKG,
                                      match_arg_name1="md.multi_thread.thread_id",
                                      match_arg1=idx+1,
                                      action_arg_name1="virtual_address",
                                      action_arg1=compute_meta_req_addr[idx] + meta_size,
                                      action_arg_name2="r_key",
                                      action_arg2=compute_meta_req_rkey[idx],
                                      action_arg_name3="len",
                                      action_arg3=32)
    
        m.add_rule_exact_read2_action_arg3(tbl_name="ti_craft_reth",
                                        action_name='ai_craft_reth',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CR_ACK,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name1="virtual_address",
                                        action_arg1=compute_meta_resp_addr[idx],
                                        action_arg_name2="r_key",
                                        action_arg2=compute_meta_resp_rkey[idx],
                                        action_arg_name3="len",
                                        action_arg3=16)
        
        m.add_rule_exact_read2_action_arg3(tbl_name="ti_craft_reth",
                                        action_name='ai_craft_reth',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CQ_HEAD,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name1="virtual_address",
                                        action_arg1=compute_meta_req_addr[idx],
                                        action_arg_name2="r_key",
                                        action_arg2=compute_meta_req_rkey[idx],
                                        action_arg_name3="len",
                                        action_arg3=16)

        m.add_rule_exact_read2_action_arg7(tbl_name="ti_craft_eth_ip_udp_bth",
                                        action_name='ai_craft_eth_ip_udp_bth',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CQ,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name1="eth_src_addr",
                                        action_arg1=memory_server_ether_addr,
                                        action_arg_name2="eth_dst_addr",
                                        action_arg2=compute_server_ether_addr,
                                        action_arg_name3="ip_src_addr",
                                        action_arg3=memory_server_ip_addr,
                                        action_arg_name4="ip_dst_addr",
                                        action_arg4=compute_server_ip_addr,
                                        action_arg_name5="udp_src_port",
                                        action_arg5=compute_udp_src_port[idx],
                                        action_arg_name6="qp_num",
                                        action_arg6=compute_qp[idx],
                                        action_arg_name7="ack",
                                        action_arg7=1)

        m.add_rule_exact_read2_action_arg7(tbl_name="ti_craft_eth_ip_udp_bth",
                                        action_name='ai_craft_eth_ip_udp_bth',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CR,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name1="eth_src_addr",
                                        action_arg1=memory_server_ether_addr,
                                        action_arg_name2="eth_dst_addr",
                                        action_arg2=compute_server_ether_addr,
                                        action_arg_name3="ip_src_addr",
                                        action_arg3=memory_server_ip_addr,
                                        action_arg_name4="ip_dst_addr",
                                        action_arg4=compute_server_ip_addr,
                                        action_arg_name5="udp_src_port",
                                        action_arg5=compute_udp_src_port[idx],
                                        action_arg_name6="qp_num",
                                        action_arg6=compute_qp[idx],
                                        action_arg_name7="ack",
                                        action_arg7=1
                                        )

        m.add_rule_exact_read2_action_arg7(tbl_name="ti_craft_eth_ip_udp_bth",
                                        action_name='ai_craft_eth_ip_udp_bth',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_M,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name1="eth_src_addr",
                                        action_arg1=compute_server_ether_addr,
                                        action_arg_name2="eth_dst_addr",
                                        action_arg2=memory_server_ether_addr,
                                        action_arg_name3="ip_src_addr",
                                        action_arg3=compute_server_ip_addr,
                                        action_arg_name4="ip_dst_addr",
                                        action_arg4=memory_server_ip_addr,
                                        action_arg_name5="udp_src_port",
                                        action_arg5=compute_udp_src_port[idx],
                                        action_arg_name6="qp_num",
                                        action_arg6=memory_qp[idx],
                                        action_arg_name7="ack",
                                        action_arg7=1
                                        )
        if (idx%2 == 0):
            m.add_rule_exact_read2_action_arg7(tbl_name="ti_craft_eth_ip_udp_bth",
                                            action_name='ai_craft_eth_ip_udp_bth',
                                            match_arg_name0="md.cm_type.cm",
                                            match_arg0=CM_CQ_PKG,
                                            match_arg_name1="md.multi_thread.thread_id",
                                            match_arg1=idx+1,
                                            action_arg_name1="eth_src_addr",
                                            action_arg1=memory_server_ether_addr,
                                            action_arg_name2="eth_dst_addr",
                                            action_arg2=compute_server_ether_addr,
                                            action_arg_name3="ip_src_addr",
                                            action_arg3=memory_server_ip_addr,
                                            action_arg_name4="ip_dst_addr",
                                            action_arg4=compute_server_ip_addr,
                                            action_arg_name5="udp_src_port",
                                            action_arg5=compute_udp_src_port_p4_req,
                                            action_arg_name6="qp_num",
                                            action_arg6=compute_qp_p4_req,
                                            action_arg_name7="ack",
                                            action_arg7=1
                                            )
        else:
            m.add_rule_exact_read2_action_arg7(tbl_name="ti_craft_eth_ip_udp_bth",
                                            action_name='ai_craft_eth_ip_udp_bth',
                                            match_arg_name0="md.cm_type.cm",
                                            match_arg0=CM_CQ_PKG,
                                            match_arg_name1="md.multi_thread.thread_id",
                                            match_arg1=idx+1,
                                            action_arg_name1="eth_src_addr",
                                            action_arg1=memory_server_ether_addr,
                                            action_arg_name2="eth_dst_addr",
                                            action_arg2=compute_server_ether_addr,
                                            action_arg_name3="ip_src_addr",
                                            action_arg3=memory_server_ip_addr,
                                            action_arg_name4="ip_dst_addr",
                                            action_arg4=compute_server_ip_addr,
                                            action_arg_name5="udp_src_port",
                                            action_arg5=compute_udp_src_port_p4_req_2,
                                            action_arg_name6="qp_num",
                                            action_arg6=compute_qp_p4_req_2,
                                            action_arg_name7="ack",
                                            action_arg7=1
                                            )

    
        m.add_rule_exact_read2_action_arg7(tbl_name="ti_craft_eth_ip_udp_bth",
                                        action_name='ai_craft_eth_ip_udp_bth',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CQ_HEAD,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name1="eth_src_addr",
                                        action_arg1=memory_server_ether_addr,
                                        action_arg_name2="eth_dst_addr",
                                        action_arg2=compute_server_ether_addr,
                                        action_arg_name3="ip_src_addr",
                                        action_arg3=memory_server_ip_addr,
                                        action_arg_name4="ip_dst_addr",
                                        action_arg4=compute_server_ip_addr,
                                        action_arg_name5="udp_src_port",
                                        action_arg5=compute_udp_src_port_p4_head,
                                        action_arg_name6="qp_num",
                                        action_arg6=compute_qp_p4_head,
                                        action_arg_name7="ack",
                                        action_arg7=1
                                        )
        #if (idx%2 == 0):
        m.add_rule_exact_read2_action_arg7(tbl_name="ti_craft_eth_ip_udp_bth",
                                        action_name='ai_craft_eth_ip_udp_bth',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CR_ACK,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name1="eth_src_addr",
                                        action_arg1=memory_server_ether_addr,
                                        action_arg_name2="eth_dst_addr",
                                        action_arg2=compute_server_ether_addr,
                                        action_arg_name3="ip_src_addr",
                                        action_arg3=memory_server_ip_addr,
                                        action_arg_name4="ip_dst_addr",
                                        action_arg4=compute_server_ip_addr,
                                        action_arg_name5="udp_src_port",
                                        action_arg5=compute_udp_src_port_p4_finish,
                                        action_arg_name6="qp_num",
                                        action_arg6=compute_qp_p4_finish,
                                        action_arg_name7="ack",
                                        action_arg7=1
                                        )
        
        m.add_rule_exact_read2_action_arg(tbl_name="ti_craft_rkey_phase_2",
                                        action_name='ai_craft_rkey_phase_2',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CQ,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="r_key",
                                        action_arg=compute_data_req_rkey[idx])
        
        m.add_rule_exact_read2_action_arg(tbl_name="ti_craft_rkey_phase_2",
                                        action_name='ai_craft_rkey_phase_2',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CR,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="r_key",
                                        action_arg=compute_data_resp_rkey[idx])
        
        m.add_rule_exact_read2_action_arg(tbl_name="ti_craft_rkey_phase_2",
                                        action_name='ai_craft_rkey_phase_2',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_M,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="r_key",
                                        action_arg=memory_pool_rkey)
        if (idx%2 ==0):
            m.add_rule_exact_read2_action_arg(tbl_name="ti_craft_psn",
                                            action_name='ai_craft_psn',
                                            match_arg_name0="md.cm_type.cm",
                                            match_arg0=CM_CQ_PKG,
                                            match_arg_name1="md.multi_thread.thread_id",
                                            match_arg1=idx+1,
                                            action_arg_name="psn",
                                            action_arg=memory_psn_p4_req)
        else:
            m.add_rule_exact_read2_action_arg(tbl_name="ti_craft_psn",
                                            action_name='ai_craft_psn',
                                            match_arg_name0="md.cm_type.cm",
                                            match_arg0=CM_CQ_PKG,
                                            match_arg_name1="md.multi_thread.thread_id",
                                            match_arg1=idx+1,
                                            action_arg_name="psn",
                                            action_arg=memory_psn_p4_req_2)

        
        m.add_rule_exact_read2_action_arg(tbl_name="ti_craft_psn",
                                        action_name='ai_craft_psn',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CQ_HEAD,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="psn",
                                        action_arg=memory_psn_p4_head)
        #if (idx%2 == 0):
        m.add_rule_exact_read2_action_arg(tbl_name="ti_craft_psn",
                                        action_name='ai_craft_psn',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CR_ACK,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="psn",
                                        action_arg=memory_psn_p4_finish)


        m.add_rule_exact_read2_action_arg(tbl_name="ti_craft_psn",
                                        action_name='ai_craft_psn',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CQ,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="psn",
                                        action_arg=memory_psn[idx])
        
        m.add_rule_exact_read2_action_arg(tbl_name="ti_craft_psn",
                                        action_name='ai_craft_psn',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CR,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="psn",
                                        action_arg=memory_psn[idx])
        
        m.add_rule_exact_read2_action_arg(tbl_name="ti_craft_psn",
                                        action_name='ai_craft_psn',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_M,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="psn",
                                        action_arg=compute_psn[idx])

        if (idx%2 ==0):
            m.add_rule_exact_read2_action_arg(tbl_name="ti_compute_psn_offset",
                                            action_name='ai_compute_psn_offset',
                                            match_arg_name0="md.cm_type.cm",
                                            match_arg0=CM_CQ_PKG,
                                            match_arg_name1="md.multi_thread.thread_id",
                                            match_arg1=idx+1,
                                            action_arg_name="index",
                                            action_arg=0)
        else:
            m.add_rule_exact_read2_action_arg(tbl_name="ti_compute_psn_offset",
                                            action_name='ai_compute_psn_offset',
                                            match_arg_name0="md.cm_type.cm",
                                            match_arg0=CM_CQ_PKG,
                                            match_arg_name1="md.multi_thread.thread_id",
                                            match_arg1=idx+1,
                                            action_arg_name="index",
                                            action_arg=1)


        m.add_rule_exact_read2_action_arg(tbl_name="ti_compute_psn_offset",
                                        action_name='ai_compute_psn_offset',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CQ_HEAD,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="index",
                                        action_arg=2)
        #if (idx%2 == 0):
        m.add_rule_exact_read2_action_arg(tbl_name="ti_compute_psn_offset",
                                        action_name='ai_compute_psn_offset',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CR_ACK,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="index",
                                        action_arg=3)
        '''else:
            m.add_rule_exact_read2_action_arg(tbl_name="ti_compute_psn_offset",
                                            action_name='ai_compute_psn_offset',
                                            match_arg_name0="md.cm_type.cm",
                                            match_arg0=CM_CR_ACK,
                                            match_arg_name1="md.multi_thread.thread_id",
                                            match_arg1=idx,
                                            action_arg_name="index",
                                            action_arg=2)'''


        m.add_rule_exact_read2_action_arg(tbl_name="ti_compute_psn_offset",
                                        action_name='ai_compute_psn_offset',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CQ,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="index",
                                        action_arg= (idx+1) * 2 + 3)

        m.add_rule_exact_read2_action_arg(tbl_name="ti_compute_psn_offset",
                                        action_name='ai_compute_psn_offset',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CR,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="index",
                                        action_arg= (idx+1) * 2 + 3)
        
        m.add_rule_exact_read2_action_arg(tbl_name="ti_compute_psn_offset",
                                        action_name='ai_compute_psn_offset',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_M,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="index",
                                        action_arg=(idx+1) * 2 + 4)
    
        m.add_rule_exact_read2_action_arg(tbl_name="ti_compute_psn_offset_phase_2",
                                        action_name='ai_compute_psn_offset_phase_2',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_CQ,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="index",
                                        action_arg= (idx+1) * 2 + 3)
        m.add_rule_exact_read2_action_arg(tbl_name="ti_compute_psn_offset_phase_2",
                                        action_name='ai_compute_psn_offset_phase_2',
                                        match_arg_name0="md.cm_type.cm",
                                        match_arg0=CM_M,
                                        match_arg_name1="md.multi_thread.thread_id",
                                        match_arg1=idx+1,
                                        action_arg_name="index",
                                        action_arg=(idx+1) * 2 + 4)
    '''print("pipe0&1 re_port2ctr_")
    time.sleep(300)
    for port in range(256):
        value = m.read_reg_element_for_pipe("re_port2ctr_", port, pipeid=port_to_pipe(port))
        if value != 0:
            print(port, value)

    for port in range(256):
        value = m.read_reg_element_for_pipe("re_port2ctr_byte", port, pipeid=port_to_pipe(port))
        if value != 0:
            print(port, value)'''   
    

    ternary_matches = [
        0x0FFF,
        0xF0FF,
        0xFF0F,
        0xFFF0,
        0x00FF,
        0x0F0F,
        0x0FF0,
        0xF00F,
        0xF0F0,
        0xFF00,
        0x000F,
        0x00F0,
        0x0F00,
        0xF000,
        0x0000
    ]
    ternary_masks = [
        0x0FFF,
        0xF0FF,
        0xFF0F,
        0xFFF0,
        0x00FF,
        0x0F0F,
        0x0FF0,
        0xF00F,
        0xF0F0,
        0xFF00,
        0x000F,
        0x00F0,
        0x0F00,
        0xF000,
        0x0000
    ]
    # Emulate if-else
    priorities = [
        0x2, 0x2, 0x2, 0x2,
        0x3, 0x3, 0x3, 0x3, 0x3, 0x3,
        0x4, 0x4, 0x4, 0x4,
        0x5
    ]
    m.disconnect()


if __name__ == '__main__':
    config()
