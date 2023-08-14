import time, sys, os, re, importlib, binascii, json, logging

SDE=os.getenv('SDE')
if SDE is None:
  print("ERR: missing SDE env var")
  exit()
else:
  print("SDE: {}".format(SDE))
if not SDE.endswith("/"):
  SDE += "/"
SDE_INSTALL=SDE+"install/"

pylib_paths = ["lib/python2.7/site-packages/tofino/", "lib/python2.7/site-packages/", "lib/python2.7/site-packages/p4testutils/", "lib/python2.7/site-packages/bf-ptf/"]
pylib_paths = [SDE_INSTALL+p for p in pylib_paths]
for p in pylib_paths:
  print ("Add path: %s"%str(p))
  sys.path.append(p)

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol, TMultiplexedProtocol
from res_pd_rpc.ttypes import * # DevTarget_t
from ptf.thriftutils import * # hex_to_i16
from mirror_pd_rpc.ttypes import *
from devport_mgr_pd_rpc.ttypes import *
from pal_rpc.ttypes import * # pal_port_speed_t, pal_fec_type_t

import pal_rpc.pal as pal_i
import conn_mgr_pd_rpc.conn_mgr as conn_mgr_client_module
import mc_pd_rpc.mc as mc_client_module

from bfruntime_client_base_tests import BfRuntimeTest
import bfrt_grpc.client as gc
gc.logger.setLevel(logging.CRITICAL)


class Manager(object):
  def __init__(self, p4name):
    if p4name == None or p4name == "":
      print("ERR: empty p4name")
      return
    self.p4name = p4name
    self.connect()

    self.target = gc.Target(device_id=0, pipe_id=0xffff)
    self.bfrt_info = self.interface.bfrt_info_get(self.p4name)

    self.bw_dict = {"10G":pal_port_speed_t.BF_SPEED_10G,
                  "25G":pal_port_speed_t.BF_SPEED_25G,
		  "40G":pal_port_speed_t.BF_SPEED_40G,
		  "50G":pal_port_speed_t.BF_SPEED_50G,
		  "100G":pal_port_speed_t.BF_SPEED_100G}

    self.fec_dict = {"NONE" :pal_fec_type_t.BF_FEC_TYP_NONE,
                   "FC" : pal_fec_type_t.BF_FEC_TYP_FIRECODE,
		   "RS" : pal_fec_type_t.BF_FEC_TYP_REED_SOLOMON}

  def connect(self):
    self.grpc_connect()
    self.fixed_function_connect()
    print("Done connect")

  def grpc_connect(self):
    grpc_addr = 'localhost:50052' 
    client_id = 0
    print ("setting up gRPC client interface...")
    self.interface = gc.ClientInterface(grpc_addr, 
        client_id = client_id, device_id=0,
        notifications=None) 
    self.interface.bind_pipeline_config(self.p4name)
    print("Done grpc connect")

  def fixed_function_connect(self):
    self.transport = TTransport.TBufferedTransport(TSocket.TSocket('localhost', 9090))
    self.transport.open()
    bprotocol = TBinaryProtocol.TBinaryProtocol(self.transport)

    self.pal = pal_i.Client(TMultiplexedProtocol.TMultiplexedProtocol(bprotocol, "pal"))
    self.conn = conn_mgr_client_module.Client(TMultiplexedProtocol.TMultiplexedProtocol(bprotocol, "conn_mgr"))
    self.mc = mc_client_module.Client(TMultiplexedProtocol.TMultiplexedProtocol(bprotocol, "mc"))

    # session and device handlers
    self.conn_hdl = self.conn.client_init()
    self.mc_sess_hdl = self.mc.mc_create_session()  
    self.dev_tgt = DevTarget_t(0, hex_to_i16(0xFFFF))
    print("Done fixed function connect")

  def disconnect(self):
    # grpc 
    self.interface._tear_down_stream()

    # fixed function
    self.mc.mc_destroy_session(self.mc_sess_hdl)
    self.conn.complete_operations(self.conn_hdl)
    self.conn.client_cleanup(self.conn_hdl)        
    self.transport.close()
    print ("mgr.py disconnect complete.")

  def port_up(self, dpid, rate, fec_type):  
    print ("bringing port %s up"%dpid)
    self.pal.pal_port_add(0, dpid, rate, fec_type)
    # disable and then enable auto-negotiation
    self.pal.pal_port_an_set(0, dpid, 2)
    self.pal.pal_port_enable(0, dpid)
    self.pal.pal_port_an_set(0, dpid, 1)

  def enab_ports_sym_bw(self, enab_ports, bw):
    bwconfig = self.bw_dict[bw]
    print("Enable ports: "+str(enab_ports)+" with speed "+bw)
    try:
      for port in enab_ports:
        self.pal.pal_port_add(0, port, bwconfig, pal_fec_type_t.BF_FEC_TYP_NONE)
        self.pal.pal_port_enable(0, port)
      print("Port configuration suceeds")
    except:
      print("Port configuration fails")
  def enab_ports_sym_bw_rs(self, enab_ports, bw):
    bwconfig = self.bw_dict[bw]
    print("Enable ports: "+str(enab_ports)+" with speed "+bw)
    try:
      for port in enab_ports:
	self.pal.pal_port_add(0, port, bwconfig, pal_fec_type_t.BF_FEC_TYP_REED_SOLOMON)
	print("Use RS FEC")
        self.pal.pal_port_enable(0, port)
      print("Port configuration suceeds")
    except:
      print("Port configuration fails")

  def create_mc_grp(self, mc_gid, dpids):
    lag_map = set_port_or_lag_bitmap(256, [])
    flood_ports = [int(p) for p in dpids]
    print("mc gid: %s; ports: %s"%(mc_gid, str(dpids)))
    port_map = set_port_or_lag_bitmap(288, list(flood_ports))
    mc_grp_hdl = self.mc.mc_mgrp_create(self.mc_sess_hdl, self.dev_tgt.dev_id, hex_to_i16(mc_gid))
    mc_node_hdl = self.mc.mc_node_create(self.mc_sess_hdl, self.dev_tgt.dev_id, 0, port_map, lag_map)
    self.mc.mc_associate_node(self.mc_sess_hdl, self.dev_tgt.dev_id, mc_grp_hdl, mc_node_hdl, 0, 0)
    # mc.mc_mgrp_destroy(mc_sess_hdl, dev_tgt.dev_id, hex_to_i32(mc_grp_hdl))
    self.conn.complete_operations(self.conn_hdl)
    self.mc.mc_complete_operations(self.mc_sess_hdl)
    return (mc_node_hdl, mc_grp_hdl)

  def add_multinode_mc_grp(self, mc_gid, dpids_and_rids):
    # create the mc group
    mc_grp_hdl = self.mc.mc_mgrp_create(self.mc_sess_hdl, self.dev_tgt.dev_id, hex_to_i16(mc_gid))
    # add one node for each (dpid, rid) pair
    lag_map = set_port_or_lag_bitmap(256, [])
    mc_node_hdls = []
    for (dpid, rid) in dpids_and_rids:
      port_map = set_port_or_lag_bitmap(288, [dpid])
      mc_node_hdl = self.mc.mc_node_create(self.mc_sess_hdl, self.dev_tgt.dev_id, rid, port_map, lag_map)
      self.mc.mc_associate_node(self.mc_sess_hdl, self.dev_tgt.dev_id, mc_grp_hdl, mc_node_hdl, 0, 0)
      mc_node_hdls.append(mc_node_hdl)
    return mc_node_hdls, mc_grp_hdl

  def read_reg_element_for_pipe(self, regname, index, pipeid):
    register_table = self.bfrt_info.table_get(regname)
    resp = register_table.entry_get(
                   self.target,
		   [register_table.make_key([gc.KeyTuple('$REGISTER_INDEX', index)])],
                   {"from_hw": True})
    data, key = resp.next()
    dataobj = data.field_dict.values()[0]
    field = dataobj.name
    val = data._get_val(dataobj)
    return val[pipeid]

  def set_default_rule_action_arg(self, tbl_name, action_name, action_arg_name, action_arg):
    table = self.bfrt_info.table_get(tbl_name)
    action_data = table.make_data(
      action_name=action_name,
      data_field_list_in=[gc.DataTuple(name=action_arg_name, val=action_arg)]
    )
    table.default_entry_set(
      target=self.target,
      data=action_data
    )

  def add_rule_ternary_read_action_arg(self, tbl_name, action_name, match_arg_name, match_arg_ternary, match_arg_ternary_mask, match_arg_ternary_annotation, priority, action_arg_name, action_arg):
    table = self.bfrt_info.table_get(tbl_name)
    if not match_arg_ternary_annotation is None:
      table.info.key_field_annotation_add(match_arg_name, match_arg_ternary_annotation)
    table.entry_add(
      self.target,
      [table.make_key([gc.KeyTuple(match_arg_name, match_arg_ternary, match_arg_ternary_mask),
	  gc.KeyTuple('$MATCH_PRIORITY', priority)])],
      [table.make_data([gc.DataTuple(action_arg_name, action_arg)], action_name)],
    ) 
  def add_rule_ternary_read2_action_arg(self, tbl_name, action_name, match_arg_name0, match_arg_ternary0, match_arg_ternary_mask0, match_arg_name1, match_arg_ternary1, match_arg_ternary_mask1,
  match_arg_ternary_annotation, priority, action_arg_name, action_arg):
    table = self.bfrt_info.table_get(tbl_name)
    if not match_arg_ternary_annotation is None:
      table.info.key_field_annotation_add(match_arg_name0, match_arg_ternary_annotation)
      table.info.key_field_annotation_add(match_arg_name1, match_arg_ternary_annotation)
    table.entry_add(
      self.target,
      [table.make_key([gc.KeyTuple(match_arg_name0, match_arg_ternary0, match_arg_ternary_mask0),
      gc.KeyTuple(match_arg_name1, match_arg_ternary1, match_arg_ternary_mask1),
	  gc.KeyTuple('$MATCH_PRIORITY', priority)])],
      [table.make_data([gc.DataTuple(action_arg_name, action_arg)], action_name)],
    )

  def add_rule_exact_read_ternary_read(self, tbl_name, action_name, match_arg_exact_name, match_arg_exact, match_arg_ternary_name, match_arg_ternary, match_arg_ternary_mask, match_arg_ternary_annotation, priority):
    table = self.bfrt_info.table_get(tbl_name)
    if not match_arg_ternary_annotation is None:
      table.info.key_field_annotation_add(match_arg_ternary_name, match_arg_ternary_annotation)
    table.entry_add(
      self.target,
      [table.make_key([
          gc.KeyTuple(match_arg_exact_name, match_arg_exact),
          gc.KeyTuple(match_arg_ternary_name, match_arg_ternary, match_arg_ternary_mask),
	  gc.KeyTuple('$MATCH_PRIORITY', priority)])],
      [table.make_data([], action_name)],
    ) 

  def add_rule_exact_read(self, tbl_name, action_name, match_arg_exact_name, match_arg_exact):
    table = self.bfrt_info.table_get(tbl_name)
    table.entry_add(
      self.target,
      [table.make_key([
          gc.KeyTuple(match_arg_exact_name, match_arg_exact)])],
      [table.make_data([], action_name)],
    ) 


  def add_rule_exact_read_ternary_read_action_arg(self, tbl_name, action_name, match_arg_exact_name, match_arg_exact, match_arg_ternary_name, match_arg_ternary, match_arg_ternary_mask, match_arg_ternary_annotation, priority, action_arg_name, action_arg):
    table = self.bfrt_info.table_get(tbl_name)
    if not match_arg_ternary_annotation is None:
      table.info.key_field_annotation_add(match_arg_ternary_name, match_arg_ternary_annotation)
    table.entry_add(
      self.target,
      [table.make_key([
          gc.KeyTuple(match_arg_exact_name, match_arg_exact),
          gc.KeyTuple(match_arg_ternary_name, match_arg_ternary, match_arg_ternary_mask),
	  gc.KeyTuple('$MATCH_PRIORITY', priority)])],
      [table.make_data([gc.DataTuple(action_arg_name, action_arg)], action_name)],
    ) 

  def add_rule_exact_read2_action_arg(self, tbl_name, action_name, match_arg_name0, match_arg0, match_arg_name1, match_arg1, action_arg_name, action_arg):
    table = self.bfrt_info.table_get(tbl_name)
    table.entry_add(
      self.target,
      [table.make_key([gc.KeyTuple(match_arg_name0, match_arg0),
                       gc.KeyTuple(match_arg_name1, match_arg1)])],
      [table.make_data([gc.DataTuple(action_arg_name, action_arg)], action_name)],
    )
  
  def add_rule_exact_read_action_arg(self, tbl_name, action_name, match_arg_name0, match_arg0, action_arg_name, action_arg):
    table = self.bfrt_info.table_get(tbl_name)
    table.entry_add(
      self.target,
      [table.make_key([gc.KeyTuple(match_arg_name0, match_arg0)])],
      [table.make_data(
        [gc.DataTuple(action_arg_name, action_arg)], action_name)],
    )

  def add_rule_exact_read_action_arg2(self, tbl_name, action_name, match_arg_name0, match_arg0, action_arg_name1, action_arg1, action_arg_name2, action_arg2):
    table = self.bfrt_info.table_get(tbl_name)
    table.entry_add(
      self.target,
      [table.make_key([gc.KeyTuple(match_arg_name0, match_arg0)])],
      [table.make_data(
        [gc.DataTuple(action_arg_name1, action_arg1), gc.DataTuple(action_arg_name2, action_arg2)], action_name)],
    )

  def add_rule_exact_read_action_arg3(self, tbl_name, action_name, match_arg_name0, match_arg0, action_arg_name1, action_arg1, action_arg_name2, action_arg2, action_arg_name3, action_arg3):
    table = self.bfrt_info.table_get(tbl_name)
    table.entry_add(
      self.target,
      [table.make_key([gc.KeyTuple(match_arg_name0, match_arg0)])],
      [table.make_data(
        [gc.DataTuple(action_arg_name1, action_arg1), gc.DataTuple(action_arg_name2, action_arg2), gc.DataTuple(action_arg_name3, action_arg3)], action_name)],
  )

  def add_rule_exact_read_action_arg7(self, tbl_name, action_name, match_arg_name0, match_arg0, action_arg_name1, action_arg1, action_arg_name2, action_arg2, action_arg_name3, action_arg3,
  action_arg_name4, action_arg4, action_arg_name5, action_arg5, action_arg_name6, action_arg6, action_arg_name7, action_arg7): 
    table = self.bfrt_info.table_get(tbl_name)
    table.entry_add(
      self.target,
      [table.make_key([gc.KeyTuple(match_arg_name0, match_arg0)])],
      [table.make_data(
        [gc.DataTuple(action_arg_name1, action_arg1), gc.DataTuple(action_arg_name2, action_arg2), gc.DataTuple(action_arg_name3, action_arg3), 
        gc.DataTuple(action_arg_name4, action_arg4), gc.DataTuple(action_arg_name5, action_arg5), gc.DataTuple(action_arg_name6, action_arg6), gc.DataTuple(action_arg_name7, action_arg7)], action_name)],
  )
  def add_rule_exact_read2_action_arg3(self, tbl_name, action_name, match_arg_name0, match_arg0, match_arg_name1, match_arg1, action_arg_name1, action_arg1, action_arg_name2, action_arg2, action_arg_name3, action_arg3):
    table = self.bfrt_info.table_get(tbl_name)
    table.entry_add(
      self.target,
      [table.make_key([gc.KeyTuple(match_arg_name0, match_arg0),
                       gc.KeyTuple(match_arg_name1, match_arg1)])],
      [table.make_data(
        [gc.DataTuple(action_arg_name1, action_arg1), gc.DataTuple(action_arg_name2, action_arg2), gc.DataTuple(action_arg_name3, action_arg3)], action_name)],
  )
  def add_rule_exact_read2_action_arg7(self, tbl_name, action_name, match_arg_name0, match_arg0, match_arg_name1, match_arg1, action_arg_name1, action_arg1, action_arg_name2, action_arg2, action_arg_name3, action_arg3,
  action_arg_name4, action_arg4, action_arg_name5, action_arg5, action_arg_name6, action_arg6, action_arg_name7, action_arg7): 
    table = self.bfrt_info.table_get(tbl_name)
    table.entry_add(
      self.target,
      [table.make_key([gc.KeyTuple(match_arg_name0, match_arg0),
                       gc.KeyTuple(match_arg_name1, match_arg1)])],
      [table.make_data(
        [gc.DataTuple(action_arg_name1, action_arg1), gc.DataTuple(action_arg_name2, action_arg2), gc.DataTuple(action_arg_name3, action_arg3), 
        gc.DataTuple(action_arg_name4, action_arg4), gc.DataTuple(action_arg_name5, action_arg5), gc.DataTuple(action_arg_name6, action_arg6), gc.DataTuple(action_arg_name7, action_arg7)], action_name)],
  )

  def print_pktgen_counter_for_app(self, appid):
    print("App "+str(appid)+":"+str(self.conn.pktgen_get_pkt_counter(self.conn_hdl, self.dev_tgt, appid)))

  def delete_mc_group(self, mc_grp_hdl):
    self.mc.mc_mgrp_destroy(self.mc_sess_hdl, self.dev_tgt.dev_id, hex_to_i32(mc_grp_hdl))

  def addExactEntry(self, tableName, fieldNames, fieldVals, actionName, actionArgs={}):
    print ("adding exact entry: {0}[{1}=={2}] --> {3}({4})".format(tableName, str(fieldNames), str(fieldVals), actionName, actionArgs))
    table = self.bfrt_info.table_get(tableName)        
    key_list = [table.make_key([gc.KeyTuple(fieldName, fieldVal)]) for (fieldName, fieldVal) in zip(fieldNames, fieldVals)]
    data_list = []
    for argName, argVal in actionArgs.items():
        data_list.append(table.make_data([gc.DataTuple(argName, argVal)], actionName))
    table.entry_add(self.target,key_list, data_list)


# helpers 
def port_to_pipe(port):
  return port >> 7
def port_to_pipe_local_id(port):
  return port & 0x7F
def port_to_bit_idx(port):
  pipe = port_to_pipe(port)
  index = port_to_pipe_local_id(port)
  return 72 * pipe + index
def set_port_or_lag_bitmap(bit_map_size, indicies):
  bit_map = [0] * ((bit_map_size+7)/8)
  for i in indicies:
    index = port_to_bit_idx(i)
    bit_map[index/8] = (bit_map[index/8] | (1 << (index%8))) & 0xFF
  return bytes_to_string(bit_map)

def create_dir(name):
  if not os.path.exists(name):
    os.mkdir(name)
    print("mkdir {}".format(name))
  else:
    print("{} exists".format(name))
