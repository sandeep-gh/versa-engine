import base64
from io import BytesIO, TextIOWrapper
from versa_engine.csv_reader import csv_infer_schema as cis
import os
#print("filesize in bytes = ",  os.stat("x.csv").st_size)

from versa_engine.csv_reader import csv_infer_schema as cis
import glob

source_dirbase = "/home/kabira/Data/csvs_for_versa_testing"
for _fn in glob.glob(f"{source_dirbase}/*.csv"):
    print(_fn)
    try:
        data = open(_fn, 'rb').read()
        r = cis.get_csv_report(data)
        print(r)
    except UnicodeDecodeError:
        print("unable to parse with encoding ", _fn)
