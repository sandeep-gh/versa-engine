import io
import os
import re
from typing import NamedTuple, Any
from enum import Enum, auto
from xml.dom import minidom
from string import Template
import csv
import strconv
from charset_normalizer import from_bytes, from_path
from file_read_backwards import FileReadBackwards
from io import BytesIO, TextIOWrapper
import codecs
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
sniffer = csv.Sniffer()

supported_encodings = ["utf-8", "ascii", "latin-1", "utf_8"]


class ParseException(Exception):
    pass


class ScannerReport(NamedTuple):
    row: Any
    dialect: Any
    cols_type: Any
    chars: Any
    pass


class CSV_Report(NamedTuple):
    delimiter: str
    delimiter_name: str
    num_header_lines: int
    cols_type: Any
    dialect: Any
    header_candidates: Any
    samples: Any
    header_lines: Any
    num_data_lines: Any
    pass


class ValidateStatus(Enum):
    NumColumnFail = auto()
    ReachedEOF = auto()
    ColumnSchemaMismatch = auto()
    ParseLineLimit = auto()


BLOCKSIZE = 1048576  # or some other, desired size in bytes


def encode_utf8(fn, encoding, outfn):
    with codecs.open(fn, "r", encoding) as sourceFile:
        with codecs.open(outfn, "w", "utf-8") as targetFile:
            while True:
                contents = sourceFile.read(BLOCKSIZE)
                if not contents:
                    break
                targetFile.write(contents)


def get_datastream_size(fn_or_bytes):
    if isinstance(fn_or_bytes, str):
        return os.stat(fn_or_bytes).st_size - 2
    if isinstance(fn_or_bytes, bytes):
        # reason for -2 unclear; we are getting more lines then actual
        return len(fn_or_bytes) - 2


def inferschema_line(l, expected_cols_type=None, use_dialect=None):
    if use_dialect is None:
        use_dialect = sniffer.sniff(l)

    row = csv.reader([l], dialect=use_dialect).__next__()

    if expected_cols_type is None:
        cols_type = [x[1]
                     for x in strconv.convert_series(row, include_type=True)]
    else:
        cols_type = [x[1]
                     for x in strconv.convert_series_with_type(row, include_type=True, types=expected_cols_type)]

    return ScannerReport(row, use_dialect, cols_type, len(l))
#['string' if _ is None else _ for _ in cols_type]


def validateschema_reader(linereader,  expected_cols_type, dialect):
    line_num = 0
    for l in linereader:
        if l != '':
            ar = inferschema_line(l,  expected_cols_type, dialect)
            line_num = line_num + 1
            yield ar


def inferschema_reader(linereader, use_dialect=None):
    '''
    x: something that treats io.BytesIO and file-stream on equal footing
    '''
    for l in linereader:

        if l != '':
            yield inferschema_line(l, use_dialect=use_dialect)


def infer_encoding(fn_or_bytes):
    if isinstance(fn_or_bytes, str):
        encoding = from_path(
            fn_or_bytes
        ).best().encoding
        return encoding
    if isinstance(fn_or_bytes, bytes):
        encoding = from_bytes(
            fn_or_bytes
        ).best().encoding
        return encoding


def forward_reader(fn_or_bytes, encoding='utf-8'):
    '''
    fn_or_cp: file name or content pointer (a bytesio object)
    '''

    if isinstance(fn_or_bytes, str):
        return open(fn_or_bytes, 'r', encoding=encoding)
    if isinstance(fn_or_bytes, bytes):
        bp = BytesIO(fn_or_bytes)
        bp.seek(0, io.SEEK_SET)
        return io.TextIOWrapper(bp, encoding=encoding)


def reverse_reader(fn_or_bytes, encoding='utf-8'):
    if isinstance(fn_or_bytes, str):
        if encoding not in supported_encodings:
            # TODO: clean up resources
            encode_utf8(fn_or_bytes, encoding, f"{fn_or_bytes}.encoded")
            bp = f"{fn_or_bytes}.encoded"
            encoding = 'utf-8'
        else:
            bp = fn_or_bytes
    if isinstance(fn_or_bytes, bytes):
        if encoding not in supported_encodings:
            tt = fn_or_bytes.decode(encoding)
            encoding = "utf-8"
            fn_or_bytes = tt.encode(encoding)
        bp = BytesIO(fn_or_bytes)
        bp.seek(0, io.SEEK_SET)

    return FileReadBackwards(bp, encoding=encoding)


# def schema_post_process():
#     for idx, (ct1, ct2) in enumerate(zip(cols_type, re.cols_type)):
#         if ct1 != ct2:
#             cols_type[idx] = "string"

#     for idx, (ct1, ct2) in enumerate(zip(rs.cols_type, re.cols_type)):
#         if ct1 != ct2:
#             cols_type[idx] = "string"

def concile_schema(expected_cols_type, rs_cols, rs_row, re_cols, re_row, hasnulls):
    cct = []  # conciled column type
    for idx, (ec, rs, re) in enumerate(zip(expected_cols_type, rs_cols, re_cols)):
        if len(set([ec, rs, re])) != 1:  # TODO: change to match stmt
            rst = strconv.convert(rs_row[idx], include_type=True)
            ret = strconv.convert(re_row[idx], include_type=True)
            if rst[1] in ['int', 'float'] and ret[1] in ['int', 'float']:
                cct.append('float')
            elif None in [rst[1], ret[1]]:
                cct.append(None)
            elif rst[1] == 'EmptyString' and ret[1] == 'EmptyString':
                hasnulls[idx] = True
                cct.append('EmptyString')
            elif rst[1] == 'EmptyString':
                hasnulls[idx] = True
                cct.append(ret[1])
                hasnulls[idx] = True
            elif ret[1] == 'EmptyString':
                cct.append(rst[1])
                hasnulls[idx] = True
            else:
                print("col ", idx, " to string ", rs_row, " ", re_row)
                raise ParseException(
                    f"A:unable to concile types {rst[1]} and {ret[1]}")
                cct.append(None)
        else:
            cct.append(expected_cols_type[idx])
    return cct


def check_schema_concile(cl1, cl2, hasnulls):
    cl = []
    for idx, (t1, t2) in enumerate(zip(cl1, cl2)):
        if t1 == t2:
            continue
        if t1 is None and t2 is None:
            continue
        if t1 in ['int', 'float'] and t2 in ['int', 'float']:
            continue
        if t1 in ['datetime', 'DateInterval'] and t2 in ['datetime', 'DateInterval']:
            continue
        if 'EmptyString' in [t1, t2]:
            hasnulls[idx] = True
            continue
        return False

    return True


def concile_schema_trivial(cl1, cl2, hasnulls):
    cl = []
    for idx, (t1, t2) in enumerate(zip(cl1, cl2)):
        if t1 == t2:
            cl.append(t1)
            continue
        if t1 is None or t2 is None:
            cl.append(None)
            continue
        if t1 in ['int', 'float'] and t2 in ['int', 'float']:
            cl.append('float')
            continue
        if t1 in ['datetime', 'DateInterval'] and t2 in ['datetime', 'DateInterval']:
            cl.append('DateInterval')
            continue

        if 'EmptyString' in [t1, t2]:
            if t1 == 'EmptyString':
                cl.append(t2)
            else:
                cl.append(t1)
            hasnulls[idx] = True
            continue

        raise ParseException(f"unable to concile types {t1} and {t2}")
    return cl


def get_csv_report(csvstore):
    '''
    csvfilename: something that takes io.BytesIO and filehandle on equal footing
    '''
    logger.setLevel(logging.INFO)
    cols_type = None
    parse_status = True
    header_candidates = []
    num_header_lines = 0
    datastream_size = get_datastream_size(csvstore)
    encoding = infer_encoding(csvstore)
    _flr = forward_reader(csvstore, encoding)
    _rlr = reverse_reader(csvstore, encoding)

    reverse_inferer = inferschema_reader(_rlr)
    samples = []
    chars_read = 0
    header_lines = []
    re = next(reverse_inferer)
    expected_cols_type = re.cols_type
    hasnulls = [False] * len(expected_cols_type)
    while True:
        if 'EmptyString' in expected_cols_type:
            try:
                re = next(reverse_inferer)
                expected_cols_type = concile_schema_trivial(
                    expected_cols_type, re.cols_type, hasnulls)
                print("ee : ", expected_cols_type)
            except StopIteration:
                # raise ParseException(
                #    f"No suitable rows:  all rows have at least one empty column")
                _rlr = reverse_reader(csvstore, encoding)
                reverse_inferer = inferschema_reader(
                    _rlr)  # settle with empty string only
                break
        else:
            _rlr = reverse_reader(csvstore, encoding)
            reverse_inferer = inferschema_reader(_rlr)  # start scan all over
            break

    #logger.info("last line", re.row, " ", re.cols_type)

    dialect = re.dialect
    # samples.append(re.row)
    num_lines = 1
    chars_read += re.chars
    header_zone = True

    forward_inferer = inferschema_reader(_flr, dialect)
    for rs in forward_inferer:
        if check_schema_concile(rs.cols_type, expected_cols_type, hasnulls):
            samples.append(rs.row)
            header_zone = False

        chars_read += rs.chars

        if not header_zone:
            break
        if(len(set(rs.row)) == len(rs.cols_type)):
            # column names cannot have spaces
            header_candidates.append([_.replace(" ", "") for _ in rs.row])

        num_header_lines = num_header_lines + 1
        header_lines.append(rs.row)
        print("num_lines = ", num_header_lines)
    num_lines += num_header_lines

    def validateschema(forward_linereader, reverse_linereader, expected_cols_type, hasnulls):
        '''
        '''
        nonlocal chars_read
        nonlocal num_lines
        rs = None
        re = None
        for rs, re in zip(validateschema_reader(forward_linereader,  expected_cols_type, dialect),
                          validateschema_reader(reverse_linereader,  expected_cols_type, dialect)):

            chars_read += rs.chars
            chars_read += re.chars
            num_lines += 2
            print("num_lines = ", num_lines)
            if len(expected_cols_type) != len(re.cols_type):
                return [ValidateStatus.NumColumnFail, rs, re]

            if len(rs.cols_type) != len(re.cols_type):
                return [ValidateStatus.NumColumnFail, rs, re]

            if rs.cols_type != re.cols_type:
                return [ValidateStatus.ColumnSchemaMismatch, rs, re]

            if len(samples) < 10:
                samples.append(rs.row)
                samples.append(re.row)
            if chars_read >= datastream_size:
                return [ValidateStatus.ReachedEOF, rs, re]

            if num_lines > 4294967296:
                return [ValidateStatus.ParseLineLimit, rs, re]
        return [ValidateStatus.ReachedEOF, rs, re]

    expected_cols_type = concile_schema_trivial(
        rs.cols_type, expected_cols_type, hasnulls)

    report_status = False

    # === scan until eof is reach; reconcile schema as you go along ==
    while True:

        if chars_read >= datastream_size:
            break
        rr = validateschema(_flr, _rlr, expected_cols_type, hasnulls)
        parse_status, rs, re = rr

        if parse_status == ValidateStatus.ReachedEOF:
            report_status = True
            break

        if parse_status == ValidateStatus.ParseLineLimit:
            report_status = True
            break

        if parse_status == ValidateStatus.NumColumnFail:
            report_status = False
            break

        if parse_status == ValidateStatus.ColumnSchemaMismatch:
            expected_cols_type = concile_schema(
                expected_cols_type, rs.cols_type, rs.row, re.cols_type, re.row, hasnulls)
            report_status = True

        pass
    # =========================== end while ==========================
    #sketches = {}
    #sketch[col_pos] = sketch
    # duplicate_cols = []
    # for col_pos in range(0,num_cols):
    #     if expected_cols_type[col_pos] == float:
    #         sketch.remove(col_pos)

    # for col_pos in sketch.keys():
    #     for _ in [rs, re]:
    #         if _.row not in sketch[col_pos]:
    #             sketch[col_pos].add(_.row)
    #         else:
    #             sketch.remove(col_pos)
    #             duplicate_cols.append(col_pos)

    if report_status:
        cols_type = ['string' if _ is None else _ for _ in expected_cols_type]
        print("hasnulls = ", hasnulls)
        if not header_candidates:
            header_candidates = [[f'col{_}' for _ in range(len(cols_type))]]
        delimiter_name = ""
        if rs.dialect.delimiter == ",":
            delimiter_name = "comma"
        if rs.dialect.delimiter == " ":
            delimiter_name = "space"
        if rs.dialect.delimiter == "|":
            delimiter_name = "pipe"

        return CSV_Report(rs.dialect.delimiter, delimiter_name, num_header_lines, cols_type, rs.dialect, header_candidates, samples, header_lines, num_lines)
    raise ParseException("Mismatch in number of columns")


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
    # escape ampersend if any -- should be replaced much earlier
    _ = _.replace('&', '&amp;')
    md_str = minidom.parseString(_).toprettyxml()
    md_str = re.sub(r'\n+', '\n', md_str).strip()
    # if metadata_path is None:
    #     metadata_path = os.path.dirname(
    #         file_path) + "/" + model_name + '.md'
    # with open(metadata_path, 'w+') as md_fh:
    #     md_fh.write(md_str)
    return md_str
