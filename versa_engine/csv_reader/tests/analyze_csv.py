from versa_engine.csv_reader import csv_infer_schema as cis
import glob
import logging
logging.basicConfig(level=logging.INFO)
#import importlib
# importlib.reload(cis)
source_dirbase = "/home/kabira/Data/csvs_for_versa_testing"
for _fn in glob.glob(f"{source_dirbase}/debug.csv"):
    print(_fn)
    try:
        r = cis.get_csv_report(f"{_fn}")
        print(r)
    except UnicodeDecodeError:
        print("unable to parse with encoding ", _fn)
