import base64
from io import BytesIO, TextIOWrapper
import csv_infer_schema as cis
import os
#print("filesize in bytes = ",  os.stat("x.csv").st_size)
data = open("/home/kabira/Datasets/2014_Adult_HIV_prevalence_rate_by_County.csv", 'rb').read()
estr = base64.b64encode(data)
bstr = base64.b64decode(estr)
#bp = BytesIO(bstr)

#print("stream size in bytes ", bp.getbuffer().nbytes)
#ssz = bp.getbuffer().nbytes
#tp = TextIOWrapper(bp, encoding='utf-8')

#read_chars = 0
# for l in tp:
#    read_chars += len(l)
#num_lines = 0
# with open("x.csv", 'r') as fh:
#     for l, r in zip(fh, tp):
#         read_chars = read_chars + len(l) + len(r)
#         if read_chars > ssz:
#             break
#         num_lines += 2

# print(r)
# print(read_chars)
# print(num_lines)
r = cis.get_csv_report(bstr)
print(r)
#assert (r is not None)
