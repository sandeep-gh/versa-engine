import base64
from io import BytesIO, TextIOWrapper
import csv_infer_schema as cis
import os
import io

data = open("/home/kabira/Datasets/2014_Adult_HIV_prevalence_rate_by_County.csv", 'rb').read()
estr = base64.b64encode(data)
bstr = base64.b64decode(estr)
bp = BytesIO(bstr)
fn_or_cp = TextIOWrapper(bp, encoding='utf-8')
fn_or_cp.seek(0, io.SEEK_SET)

_flr = fn_or_cp

for l in _flr:
    print ("line = ", l)
