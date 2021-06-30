import csv
import os
from string import Template


def is_int_format(val):
    try:
        a = int(val)
        return True
    except ValueError:
        return False


def is_float_format(val):
    try:
        a = float(val)
        return True
    except ValueError:
        return False


def get_format(val, run_type):
    '''run_type: type known so far'''
    if run_type == 'number':
        if is_int_format(val):
            return 'number'

    if run_type in ['number', 'float']:
        if is_float_format(val):
            return 'float'

    return 'string'


def build_csv_metadata(model_name=None, file_path=None, metadata_path=None, delimiter='comma'):
    with open(file_path, newline='') as f:
        reader = csv.reader(f, delimiter=',')
        headers = reader.__next__()
        num_fields = len(headers)
        formats = ['number'] * num_fields
        has_null = ['no'] * num_fields
        for row in reader:
            for idx, val in enumerate(row):
                val = val.strip()
                if val == "":
                    has_null[idx] = 'yes'
                else:
                    prev_format = formats[idx]
                    formats[idx] = get_format(val, prev_format)
                    if prev_format != formats[idx]:
                        print("changed format from ", prev_format,
                              " to ", formats[idx], "due to ", val)

        cols_md_str = ''
        for i in range(0, num_fields):
            col_name = headers[i].strip()
            f_type = formats[i]
            if_null_stmt = ''
            if has_null[i] == 'yes':
                if_null_stmt = """ has_null='True'"""
            # for f_type in formats:
            cols_md_str += Template(
                '<column${if_null_stmt}><name>${col_name}</name><type>$f_type</type></column>\n').substitute(locals())

        md_prefix = "<metadata>\n<model>${model_name}</model>\n<delimiter>${delimiter}</delimiter>\n<header>True</header>\n<columns>\n"
        md_postfix = "</columns>\n<primarykey>\n<key></key>\n</primarykey>\n</metadata>"
        md_str = Template(md_prefix + cols_md_str +
                          md_postfix).substitute(locals())
        if metadata_path is None:
            metadata_path = os.path.dirname(
                file_path) + "/" + model_name + '.md'
        with open(metadata_path, 'w+') as md_fh:
            md_fh.write(md_str)

# def build_shp_metadata(model_name=None, file_path=None, metadata_path=None):
