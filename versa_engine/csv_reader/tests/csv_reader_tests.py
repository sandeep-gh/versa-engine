from addict import Dict
import csv_reader as cr
csv_metadata = Dict()

model_name = "test_model"
file_path = "/home/kabira/DrivingRange/VERSA/x.csv"
metadata_path = "/tmp/test_model.md"

cm = csv_metadata
cm.delimiter = ","
cm.primarykeys = ["c1", "c2"]
cm.num_fields = 4
cm.col_names = ["c1", "c2", "c3", "c4"]
cm.col_types = ["int", "string", "float", "float"]
cm.has_null = [False, False, True, False]
cm.num_header_lines = 4
cr.build_csv_metadata(model_name, file_path, metadata_path, cm)
