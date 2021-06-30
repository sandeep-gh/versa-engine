import base64
from io import BytesIO, TextIOWrapper
import csv_reader as cr
import os
print("filesize in bytes = ",  os.stat("x.csv").st_size)
data = open("x.csv", 'rb').read()
estr = base64.b64encode(data)
bstr = base64.b64decode(estr)
bp = BytesIO(bstr)

print("stream size in bytes ", bp.getbuffer().nbytes)
ssz = bp.getbuffer().nbytes
tp = TextIOWrapper(bp, encoding='utf-8')

read_chars = 0
# for l in tp:
#    read_chars += len(l)
num_lines = 0
# with open("x.csv", 'r') as fh:
#     for l, r in zip(fh, tp):
#         read_chars = read_chars + len(l) + len(r)
#         if read_chars > ssz:
#             break
#         num_lines += 2

# print(r)
# print(read_chars)
# print(num_lines)
r = cr.get_csv_report(bp)
print(r)
#assert (r is not None)
