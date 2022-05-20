import logging

import os
if os:
    try:
        os.remove("launcher.log")
    except:
        pass

import sys
if sys:
    FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
    logging.basicConfig(filename="launcher.log",
                        level=logging.DEBUG, format=FORMAT)

from addict import Dict

# ======================== start versa engine and build rmo ========================
import versa_engine as ve

datacfg_basedir = "/home/kabira/Databank/versa-dl"

edcfg_dir=f"{datacfg_basedir}/edcfgs"
content_dir=f"{datacfg_basedir}/content"
metadata_dir=f"{datacfg_basedir}/metadata"


edcfg_prefix=f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE edcfg [
<!ENTITY var_metadata_dir "{metadata_dir}">
<!ENTITY var_content_dir "{content_dir}">
]>


"""

edcfg_fn = f"{edcfg_dir}/x.csvpack"
#print(edcfg_prefix + open(edcfg_fn, "r").read())
edcfg_root = ve.read_xmlstring(edcfg_prefix + open(edcfg_fn, "r").read())
#print(edcfg_root)

session_run_dir = "/tmp/tmp5gbgkb_2"
session_name = "mydbsession"
[dbadapter, dbsession] = ve.connect_dbsession(session_name, session_run_dir)
#ve.build_orms(edcfg_root, session_run_dir)
xorm  = ve.import_model('x', session_run_dir)
s1 = ve.reapi.limit(dbsession, ve.schapi.proj(dbsession, xorm, ['variable']), 20)

# ====================== build an edcfg on orm =====================
csv_fn, file_elem = ve.exapi.export_rmo(dbsession, s1, "testmodel", metadata_dir=metadata_dir,
                                        content_dir=content_dir)
edcfg_root, edcfg_include_directive = ve.exapi.build_edcfg_elem([file_elem], "testmodel.csvpack", out_dir=edcfg_dir)
print(edcfg_include_directive)
