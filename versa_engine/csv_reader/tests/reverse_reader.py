from versa_engine.csv_reader import csv_infer_schema as cis
from versa_engine.csv_reader.csv_infer_schema import *

csvstore = '/home/kabira/Data/csvs_for_versa_testing/Market.csv'
encoding = infer_encoding(csvstore)

_rlr = reverse_reader(csvstore, encoding)
reverse_inferer = inferschema_reader(_rlr)
while True:
    try:
        v = next(reverse_inferer)
    except StopIteration:
        print("This is the end")
        break

res = cis.check_schema_concile([None], ['EmptyString'])
