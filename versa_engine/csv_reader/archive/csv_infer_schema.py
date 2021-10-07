from xml.dom import minidom
from string import Template
import csv
import strconv
from file_read_backwards import FileReadBackwards
from collections import namedtuple
import io
import os
import re
scanner_res_t = namedtuple(
    "scanner_res_t", ['row', 'dialect', 'cols_type', 'line_len'])
sniffer = csv.Sniffer()


def get_datastream_size(fn_or_cp):
    if isinstance(fn_or_cp, str):
        return os.stat(fn_or_cp).st_size - 2
    if isinstance(fn_or_cp, io.BytesIO):
        # reason for -2 unclear; we are getting more lines then required
        return fn_or_cp.getbuffer().nbytes - 2


def analyze_line(l, expected_cols_type = None):

    dialect = sniffer.sniff(l)
    row = csv.reader([l], dialect=dialect).__next__()

    if expected_cols_type is None:
        cols_type = [x[1]
                     for x in strconv.convert_series(row, include_type=True)]
    else:
        cols_type = [x[1]
                     for x in strconv.convert_series_with_type(row, include_type=True, types=expected_cols_type)]
    return scanner_res_t(row, dialect, ['string' if _ is None else _ for _ in cols_type], len(l))


def scan_analyse_csv(fn, get_reader):
    line_num = 0
    last_cols_type = None
    with get_reader(fn) as r:
        for l in r:
            ar =  analyze_line(l, last_cols_type)
            last_cols_type = ar.cols_type
            line_num = line_num + 1


def forward_reader(fn_or_cp):
    '''
    fn_or_cp: file name or content pointer (a bytesio object)
    '''

    try:
        if isinstance(fn_or_cp, str):
            return open(fn_or_cp, encoding="utf-8")
        if isinstance(fn_or_cp, io.BytesIO):
            fn_or_cp.seek(0, io.SEEK_SET)
            return io.TextIOWrapper(fn_or_cp, encoding='utf-8')
    except UnicodeDecodeError:
        print ("UnicodeDecodeError..try gzip ")
        



def reverse_reader(fn_or_cp):
    if isinstance(fn_or_cp, io.BytesIO):
        fn_or_cp.seek(0, io.SEEK_SET)
    return FileReadBackwards(fn_or_cp, encoding="utf-8")


csv_report_t = namedtuple("csv_report_t", [
                          'delimiter', 'delimiter_name', 'num_header_lines', 'cols_type', 'dialect', 'header_candidates', 'samples', 'header_lines', 'num_data_lines'])


def infer_types(forward_scan_analyze, reverse_scan_analyze):
    for rs, re in zip(forward_scan_analyze, reverse_scan_analyze):
        chars_read += rs.line_len
        chars_read += re.line_len
        if cols_type is None:
            cols_type = re.cols_type

        if len(cols_type) != len(re.cols_type):
            parse_status = False
            break
        if len(rs.cols_type) != len(re.cols_type):
            parse_status = False
            break

        if chars_read > datastream_size:
            break
        for idx, (ct1, ct2) in enumerate(zip(cols_type, re.cols_type)):
            if ct1 != ct2:
                cols_type[idx] = "string"

        for idx, (ct1, ct2) in enumerate(zip(rs.cols_type, re.cols_type)):
            if ct1 != ct2:
                cols_type[idx] = "string"

        if len(samples) < 10:
            samples.append(rs.row)
            samples.append(re.row)

        #if len(header_lines) < 10:
        #    samples.append(rs.row)

        num_lines += 2

    # for rs in forward_scan_analyze:
    #     num_lines += 1

    # for re in reverse_scan_analyze:
    #     num_lines += 1    
    pass

def get_csv_report(csvfilename):

    cols_type = None
    parse_status = True
    header_candidates = []
    num_header_lines = 0
    datastream_size = get_datastream_size(csvfilename)
    forward_scan_analyze = scan_analyse_csv(csvfilename, forward_reader)
    reverse_scan_analyze = scan_analyse_csv(csvfilename, reverse_reader)
    samples = []
    chars_read = 0
    header_lines = []
    for rs, re in zip(forward_scan_analyze, reverse_scan_analyze):
        if(re.dialect.delimiter == rs.dialect.delimiter and rs.cols_type == re.cols_type):
            samples.append(rs.row)
            samples.append(re.row)
            break
        if(len(set(rs.row)) == len(rs.cols_type)):
            # column names cannot have spaces
            header_candidates.append([_.replace(" ", "") for _ in rs[0]])

        chars_read += rs.line_len
        chars_read += re.line_len
        num_header_lines = num_header_lines + 1
        header_lines.append(rs.row)

    num_lines = 2 * num_header_lines
    for rs, re in zip(forward_scan_analyze, reverse_scan_analyze):
        chars_read += rs.line_len
        chars_read += re.line_len
        if cols_type is None:
            cols_type = re.cols_type

        if len(cols_type) != len(re.cols_type):
            parse_status = False
            break
        if len(rs.cols_type) != len(re.cols_type):
            parse_status = False
            break

        if chars_read > datastream_size:
            break
        for idx, (ct1, ct2) in enumerate(zip(cols_type, re.cols_type)):
            if ct1 != ct2:
                cols_type[idx] = "string"

        for idx, (ct1, ct2) in enumerate(zip(rs.cols_type, re.cols_type)):
            if ct1 != ct2:
                cols_type[idx] = "string"

        if len(samples) < 10:
            samples.append(rs.row)
            samples.append(re.row)

        #if len(header_lines) < 10:
        #    samples.append(rs.row)

        num_lines += 2

    # for rs in forward_scan_analyze:
    #     num_lines += 1

    # for re in reverse_scan_analyze:
    #     num_lines += 1

    if parse_status:

        if not header_candidates:
            header_candidates = [[f'col{_}' for _ in range(len(cols_type))]]
        delimiter_name = ""
        if rs.dialect.delimiter == ",":
            delimiter_name = "comma"
        if rs.dialect.delimiter == " ":
            delimiter_name = "space"
        if rs.dialect.delimiter == "|":
            delimiter_name = "pipe"

        # we are getting overflow of lines; -2 is just a hack
        return csv_report_t(rs.dialect.delimiter, delimiter_name, num_header_lines, cols_type, rs.dialect, header_candidates, samples, header_lines, num_lines - 2)
    return None

@profile
def build_csv_metadata(model_name=None,  csv_metadata=None):
    '''

    '''
    cm = csv_metadata
    delimiter = cm.delimiter
    delimiter_name = cm.delimiter_name
    primarykeys = cm.primarykeys

    cols_md_str = ''
    for i in range(0, len(cm.col_names)):
        col_name = cm.col_names[i]
        f_type = cm.col_types[i]
        if_null_stmt = ''
        if cm.has_null[i] == True:
            if_null_stmt = """ has_null='True'"""
        cols_md_str += Template(
            '<column${if_null_stmt}><name>${col_name}</name><type>$f_type</type></column>\n').substitute(locals())

    num_header_lines = cm.num_header_lines

    primary_key_str = ""
    for pkey in cm.primarykeys:
        primary_key_str += f'<key>{pkey}</key>\n'

    md_prefix = "<metadata>\n<model>${model_name}</model>\n<delimiter>${delimiter_name}</delimiter>\n<columns>\n"
    md_postfix = "</columns>\n<primarykey>\n${primary_key_str}\n</primarykey>\n</metadata>"
    _ = Template(md_prefix + cols_md_str +
                 md_postfix).substitute(locals())
    md_str = minidom.parseString(_).toprettyxml()
    md_str = re.sub(r'\n+', '\n', md_str).strip()
    # if metadata_path is None:
    #     metadata_path = os.path.dirname(
    #         file_path) + "/" + model_name + '.md'
    # with open(metadata_path, 'w+') as md_fh:
    #     md_fh.write(md_str)
    return md_str
