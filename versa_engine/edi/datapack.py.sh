#!/usr/bin/env python3
import argparse
import edi.dicex_edi_impl as dei
import edi.edi_config_utils as ecu
import common.utilities as utilities
import common.xmlutils as xu
import edi.build_csv_metadata as bcm
parser = argparse.ArgumentParser()

parser.add_argument("--create", action='store_true',
                    help="create new datapack/dataconfig")

parser.add_argument("--datapack", nargs=1,
                    help="create new datapack/dataconfig")

parser.add_argument("--add_csv", nargs='?',
                    help="create new datapack/dataconfig")

args = parser.parse_args()
print(args)
datapack_name = None
if args.datapack:
    datapack_name = args.datapack[0]

if args.create:
    [edcfg, files_elem] = ecu.gen_edconfig_elem()
    with open(datapack_name + ".edcfg", "w") as fh:
        fh.write(xu.tostring(edcfg))
else:
    edcfg = xu.read_file(datapack_name + ".edcfg")

if args.add_csv:
    print("csv = ", args.add_csv)
    bcm.build_csv_metadata("test_model", args.add_csv, "test_model.md")
