from versa_engine.csv_reader import csv_infer_schema as cis
import glob
import logging
import traceback
logging.basicConfig(level=logging.INFO)
#import importlib
# importlib.reload(cis)
source_dirbase = "/home/kabira/Data/csvs_for_versa_testing/fails"
for _fn in glob.glob(f"{source_dirbase}/*.csv"):
    print(_fn)
    try:
        r = cis.get_csv_report(f"{_fn}")
        print(r)
    except UnicodeDecodeError as e:
        print("unable to decode: ", str(e))
    except cis.ParseException as e:
        print("unable to parse: ", str(e))
    except Exception as e:
        print("unable to parse: unexpected error occured ", str(e))
        traceback.print_exc()
