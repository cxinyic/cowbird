import argparse
import json
import os
import sys


def print_tbl2stage(mau_json_path):
  if not os.path.isabs(mau_json_path):
    print("ERR: ", mau_json_path) 
  print(mau_json_path)

  with open(mau_json_path, "r") as f:
    j = json.loads(f.read())
    for tbl in j["tables"]:
      print("{0}:{1}".format(tbl["name"], tbl["stage_allocation"][0]["stage_number"]))



if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  targets = parser.add_subparsers(dest="target")

  prsr_print_tbl2stage = targets.add_parser("print_tbl2stage")
  prsr_print_tbl2stage.add_argument("-i", "--input_path", type=str, required=True, help="Absolte path to mau.json")

  args = parser.parse_args()

  if args.target == "print_tbl2stage":
    print_tbl2stage(args.input_path)

