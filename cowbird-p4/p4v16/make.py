import argparse
import os
import subprocess
import sys


SDE=os.environ['SDE']
if SDE is None:
  print("ERR: missing SDE env var")
  exit()
else:
  if not SDE.endswith("/"):
    SDE += "/"
  print("SDE: {}".format(SDE))

def make_sde(root_dir, bsp_path=None, target=None):

  profile_hw = """hw_profile:
  global_configure_options: ''

  packages:
    - bf-syslibs:
      - bf_syslibs_configure_options: ''
    - bf-utils:
      - bf_utils_configure_options: ''
    - bf-diags:
      - bf_diags_configure_options: ''
    - switch-p4-16:
      - switch_configure_options: ''
      - switch_profile: x1_tofino
    - p4-examples:
      - p414
      - p416
    - ptf-modules
    - bf-drivers:
      - bf_drivers_configure_options: ''
      - bf-runtime

  package_dependencies:
    - thrift
    - grpc

  tofino_architecture: tofino
"""
  profile_sim = """vm_profile:
  global_configure_options: ''

  packages:
    - bf-syslibs:
      - bf_syslibs_configure_options: ''
    - bf-utils:
      - bf_utils_configure_options: ''
    - ptf-modules
    - bf-drivers:
      - bf_drivers_configure_options: ''
      - bf-runtime
      - p4-runtime
      - pi

  package_dependencies:
    - thrift
    - grpc

  tofino_architecture: tofino
"""

  if not os.path.exists(root_dir):
    print("{} not exists".format(root_dir))
    return 
 
  if target == "sim":
    with open(root_dir+"p4studio_build/profiles/vm_profile.yaml", "w+") as f:
      f.writelines(profile_sim)
  elif target == "hw":
    with open(root_dir+"p4studio_build/profiles/hw_profile.yaml", "w+") as f:
      f.writelines(profile_hw)

  cmd = root_dir+"p4studio_build/p4studio_build.py --use-profile "+root_dir+"p4studio_build/profiles/"
  if target == "hw":
    cmd += "hw_profile.yaml"
    if bsp_path is None:
      print("ERR: missing bsp_path")
    else:
      cmd += " --bsp-path "+bsp_path
  else:
    cmd += "vm_profile.yaml"
  print(cmd)

  subprocess.call(cmd.split())

def make_compile(p4in, out):
  if not os.path.isabs(out):
    print("ERR: {} not absolute path", out)
  if not out.endswith("/"):
    out += "/"
  cmd = "rm -rf "+out
  print(cmd)
  subprocess.call(cmd.split())

  print("Compile {0} to {1}".format(p4in, out)) 
  p4c = SDE+"install/bin/bf-p4c"
  cmd = p4c+" --verbose 3 --std p4-16 --target tofino --arch tna -o "+out+" --bf-rt-schema "+out+"bf-rt.json "+p4in+" -Xp4c \"--table-placement-in-order\""
  print(cmd)
  subprocess.call(cmd.split())

def make_switchd(switchd_in_abs, switchd_out_abs, skip, ether_type, gap, prot):
  if os.path.isabs(switchd_in_abs) and os.path.isabs(switchd_out_abs):
    pass
  else:
    print("ERR: non abs path: {}, {}".format(switchd_in_abs, switchd_out_abs))
  if not switchd_out_abs.endswith("/"):
    switchd_out += "/"

  cmd = "rm switchd"
  print(cmd)
  subprocess.call(cmd.split())

  cmd = "g++ -I"+SDE+"pkgsrc/bf-drivers/include -I"+SDE+"install/include -Wno-missing-field-initializers -Werror -Wshadow -g -O2 -std=c++11 -L"+SDE+"install/lib/ -o "+switchd_out_abs+"switchd "+switchd_in_abs+" -ldriver -lbfsys -lbfutils -lbf_switchd_lib -lm -ldl -lpthread -pthread -Wl,--disable-new-dtags -L"+SDE+"install/lib -Wl,-rpath -Wl,"+SDE+"install/lib"
  print(cmd)
  subprocess.call(cmd.split())

  cmd = "sudo "+switchd_out_abs+"switchd --install-dir "+SDE+"install --conf-file "+switchd_out_abs+"rdma_p4.conf -t "+ether_type+" -g "+gap+" -p "+prot
  print(cmd)
  if not skip:
    subprocess.call(cmd.split())

def make_kill():
  cmd = "sudo killall bf_switchd -q"
  print(cmd)
  subprocess.call(cmd.split())


if __name__ == '__main__':
  
  parser = argparse.ArgumentParser()
  subcmds = parser.add_subparsers(dest="cmd")

  cmd_sde = subcmds.add_parser("sde")
  cmd_sde.add_argument("-r", "--root_dir", type=str, required=True, help="Root dir of the sde (unzipped)")
  cmd_sde.add_argument("-b", "--bsp_path", type=str, required=False, help="Name of the bsp path")
  cmd_sde.add_argument("-t", "--target", type=str, required=True, choices=["hw", "sim"], help="For simulation or hardware")

  cmd_compile = subcmds.add_parser("compile")
  cmd_compile.add_argument("p4_in", type=str, help="Path of the p4 program to compile")
  cmd_compile.add_argument("p4_out", type=str, help="Absolute output directory for p4 compiler outout")

  cmd_switchd = subcmds.add_parser("switchd")
  cmd_switchd.add_argument("switchd_in", type=str, help="cpp switchd abs file path")
  cmd_switchd.add_argument("switchd_out", type=str, help="cpp switchd abs file path")
  cmd_switchd.add_argument("--skip", action="store_true", help="Whether to skip launching switchd")
  cmd_switchd.add_argument("-t", "--ether_type", type=str, required=False, default="0x1234", help="Ether type of SEED")
  cmd_switchd.add_argument("-g", "--gap", type=str, required=False, default="59", help="Gap of seed stream")
  cmd_switchd.add_argument("-p", "--prot", type=str, required=False, default="0x11", help="Protocol type in SEED ip header")

  cmd_kill = subcmds.add_parser("kill")

  args = parser.parse_args()

  if args.cmd == "sde":
    make_sde(args.root_dir, args.bsp_path, args.target)
  elif args.cmd == "compile":
    make_compile(args.p4_in, args.p4_out)
  elif args.cmd == "switchd":
    make_switchd(args.switchd_in, args.switchd_out, args.skip, args.ether_type, args.gap, args.prot)
  elif args.cmd == "kill":
    make_kill()

