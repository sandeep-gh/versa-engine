from versa_engine.csv_reader import csv_infer_schema as cis
from versa_engine.csv_reader.encode_utf8 import encodeutf8
import glob

source_dirbase = "/home/kabira/Data/csvs_for_versa_testing"
csvstore =f"{source_dirbase}/syria-bordercrossings-2015jun11-hiu-usdos.csv"

encoding= cis.infer_encoding(csvstore)
for a in encodeutf8(csvstore, encoding):
    print(a)
