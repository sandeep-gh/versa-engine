import sys
import os
import imp
import inspect
from string import Template
from tabulate import tabulate
from sqlalchemy import MetaData, Column, Integer, String, Table, Index, Interval, Float
from sqlalchemy.orm import class_mapper, defer, mapper
from sqlalchemy.sql.sqltypes import NullType
#import pandas as pd
#from pandas import DataFrame
from postgres_copy import copy_to, copy_from, relabel_query
from versa_engine.common  import xmlutils as xu
from versa_engine.rmo import versa_api_meta as vam
from versa_engine.dataapis import schema, relational as re, utils as vu




def print_table(session, rmo):
    """
    print contents of rmo on screen as a table
    """
    [headers, tbl] = build_tabulate(session, rmo)
    return


def export_as_dataframe(session, rmo):
    """
    export content of rmo as a dataframe
    Args:

    Returns:
    A dataframe with same tabular structure and content as rmo

    Todo: copy over column names; use more fancy from_sql command (http://stackoverflow.com/questions/29525808/sqlalchemy-orm-conversion-to-pandas-dataframe/29528804#29528804)
    """
    stmt = vam.buildstmt(session, rmo)
    data_records = [rec._asdict() for rec in session.query(stmt).all()]
    df = pd.DataFrame.from_records(data_records)
    return df

    #df = pd.read_sql(stmt.statement, query.session.bind)

    #cols = stmt.c.keys()
    # return DataFrame([ [x._asdict()[key] for key in cols] for x in session.query(stmt).all()])


def scan_as_xml(session=None, rmo=None):
    '''
    returns an xml that contains content of rmo nested in 
    top level elem <rmo></rmo>
    '''

    stmt = vam.buildstmt(session, rmo)
    return to_xml(session.query(stmt).all())


def scan_as_dictionary(session=None, rmo=None, key_attr=None):
    """
       Return rows of rmo as a dictionary


    Args:
        key_attr: The attribute used as key for the dictionary

    Example:
        scan_as_dictionary(session, person, 'pid') 
        returns
        {
        3423 => (3423, 13, M)
        3424 => (3424, 18, F)
        ...
        ...
        }

    Precondition:
        key should be the primary key of the rmo

        }
    """
    stmt = vam.buildstmt(session, rmo)
    return to_dict(session.query(stmt).all(), key_attr)


def export_rmo(session=None, rmo=None, model_name=None, export_dir="./", metadata_fn=None, csv_fn=None, delimiter=',', dry_run=False, skip_metadata=False):
    """
       export content of rmo to a file

    Args:
       metadata_fn: file name to store  metadata of the rmo
       csv_fn : file name to store  content of the rmo

    Returns:
       [csv_fn, file_elem] pair where 
       csv_fn is full path of the csv file
       file_elem is the xml config elem for the
       exported file

    Todo:
       using rmo variable name as rmo
    """
    # refactor this out

    delimiter_label = None
    if delimiter == ',':
        delimiter_label = 'comma'
    if delimiter == '|':
        delimiter_label = 'pipe'
    if delimiter == ' ':
        delimiter_label = 'space'
    assert delimiter_label is not None

    if metadata_fn is None:
        metadata_fn = export_dir + "/" + model_name + ".md"

    if csv_fn is None:
        csv_fn = export_dir + "/" + model_name + ".csv"

    if not dry_run:
        copy_to_file(session, rmo, csv_fn, delimiter=delimiter)

    if not skip_metadata:
        schema.build_metadata_for_rmo(
            session, rmo, model_name=model_name, metadata_fn=metadata_fn)

    # TODO: think about returning file element
    file_elem = xu.read_string(Template(
        '''<file><location>$csv_fn</location><metadata>$metadata_fn</metadata><model_name>$model_name</model_name><filetype><filetype>csv</filetype><strict_formatted>True</strict_formatted><delimiter>$delimiter_label</delimiter></filetype></file>''').substitute(locals()))
    # <file><location>$csv_fn</location<metadata>$</metadata><model_name></model_name></file>
    return [csv_fn, file_elem]


def export_rmos_bevy(session=None, rmos=None, generic_model_name=None, export_dir='./', metadata_dir='./', metadata_fn=None,  delimiter=',', ):
    """
    export a bunch of rmos of the same type

    Args:
    rmos: A list of rmo of same type

    generic_model_name: The model name for the unified rmo

    Returns: 
    A xml config snippet with entry for each rmo

    Todo:
    Revisit this

    Comments:
    bevy: a group of people or things of the same kind //http://www.macmillandictionary.com/us/thesaurus-category/american/collections-stores-and-sets-of-things
    action: export a list of rmos
    """

    # refactor this out
    delimiter_label = None
    if delimiter == ',':
        delimiter_label = 'comma'
    if delimiter == '|':
        delimiter_label = 'pipe'
    if delimiter == ' ':
        delimiter_label = 'space'

    assert delimiter_label is not None

    # pic a representative rmo
    rep_rmo = rmos[0][0]

    all_file_elems = None
    file_elem_block = None  # the element where the file config will be added
    if config_xml_type == 'unified':
        build_metadata_for_rmo(
            session, rep_rmo, model_name=generic_model_name, metadata_fn=metadata_fn)
        all_file_elems = xu.read_string(Template(
            '''<edconfig><files><file><model_name>${generic_model_name}</model_name><metadata>${metadata_dir}/${metadata_fn}</metadata><locations></locations><filetype><filetype>csv</filetype><strict_formatted>True</strict_formatted><delimiter>$delimiter_label</delimiter></filetype></file></files></edconfig>''').substitute(locals()))
        locs_elem_block = xu.get_elems(all_file_elems, 'locations', uniq=True)
        assert locs_elem_block is not None

    for rmo, model_name, export_dir in rmos:
        csv_fn = export_dir + "/" + model_name + ".csv"
        [xml_elem, loc_elem] = copy_to_file(
            session, rmo, csv_fn, delimiter=delimiter)
        if config_xml_type == 'unified':
            locs_elem_block.append(loc_elem)

    return all_file_elems


def build_file_element(csv_fn, metadata_fn, model_name, delimiter_label, crop_head):
    file_elem = xu.read_string(Template(
        '''<file><location>$csv_fn</location><metadata>$metadata_fn</metadata><model_name>$model_name</model_name><filetype><filetype>csv</filetype><strict_formatted>True</strict_formatted><delimiter>$delimiter_label</delimiter><crop_head>${crop_head}</crop_head> </filetype></file>''').substitute(locals()))

    return file_elem


def build_edcfg_elem(file_elems=None, out_fn=None):
    """
    create xml config element from a collection of xml file elements

    Args:
    file_elems: A list of file config elems
    Out_fn: The file name to store the generated xml config

    Returns:
    A list [all_files_elems, xi_str] where
    all_files_elems : The xml edcfg files element which contains all the file_elems
    xi_str: The xml  string the refers to the generated xml config file
    """
    all_file_elems = xu.read_string('''<edconfig><files></files></edconfig>''')
    files_elem = xu.get_elems(all_file_elems, 'files', uniq=True)
    for file_elem in file_elems:
        files_elem.append(file_elem)
    # with open(out_fn, 'w') as fh:
     #   fh.write(xu.tostring(all_file_elems))

    xi_str = Template(
        """<xi:include href="$out_fn" parse="xml"/>""").substitute(out_fn=out_fn)
    return [all_file_elems, xi_str]


def save_rmo_in_table(session=None, rmo=None,  tbl=None, cols=None):
    """
    Save rmo data in existing table tbl

    Input:
         cols:  the columns of the rmo to be save in table tbl. All columns of rmo are choosen if not specified.
    """
    if cols is None:
        cols = vam.getColNames(session, rmo)
    session.execute(tbl.insert().from_select(cols, rmo))


def materialize_rmo_bevy_iter(session=None, rmo_iter=None, name_prefix=None, indexes=None, pks=None, reload=True):
    """
    bevy implies all rmos has same schema/structure
    """

    rmol = []
    for rmo in rmo_iter:
        rmol.append(rmo)
    return materialize_rmo_bevy(session, rmol, name_prefix=name_prefix, indexes=indexes, pks=pks, reload=reload)


def materialize_rmo_bevy(session=None, rmol=None, name_prefix=None, indexes=None, pks=None, reload=True):
    """
    bevy implies all rmos has same schema/structure
    """
    tbl_name = 't_' + name_prefix
    cls_name = name_prefix
    cls_fn = name_prefix + '_model.py'

    # return if rmo is already created
    if reload:
        if vu.check_model_exists(cls_name):
            return vu.import_model(cls_name)

    materialized_class = materialize_rmo_impl(
        session=session, rmo=rmol[0], tbl_name=tbl_name, cls_name=cls_name, indexes=indexes, cls_fn=cls_fn, pks=pks, reload=True, populate_table=False)
    materialized_tbl = materialized_class.__table__  # reference to the table
    for rmi in rmol:
        if re.is_empty(session, rmi):
            print("skipping table because it is empty (an outstanding bug in sqlalchemcy; from_insert breaks down on empty tables)")
        else:
            print("bevy = ", rmi.c.keys())
            save_rmo_in_table(session, rmi, materialized_tbl)

    materialized_rmo = re.select(session, materialized_class)
    return materialized_rmo


def materialize_rmo(session=None, rmo=None, name_prefix=None, indexes=None, pks=None, reload=True):
    tbl_name = 't_' + name_prefix
    cls_name = name_prefix
    cls_fn = name_prefix + '_model.py'
    if pks is None:
        pks = vam.get_pkeys(session, rmo)
    if indexes is None:
        indexes = pks
    return materialize_rmo_impl(session=session, rmo=rmo, tbl_name=tbl_name, cls_name=cls_name, indexes=indexes, cls_fn=cls_fn, pks=pks, reload=True)


def materialize_rmo_impl(session=None, rmo=None,  tbl_name=None, cls_name=None, indexes=None, cls_fn=None, pks=None, reload=False, populate_table=True):
    """
    Create table tbl_name and save the content of an rmo in the table.
    Args:

    Output: 

    Usage: 
    """
#     if reload:
#         assert cls_fn is not None
#         try:
#             sys.path.append(os.getcwd())
#             imp.find_module(cls_fn[:-3]) #removing the .py extension
#             pymodule = __import__(cls_fn[:-3])
#             rmo = getattr(pymodule, cls_name) #ignore this line for now--workaround to load module
#             assert rmo is not None
#             return rmo
#         except ImportError:
#             print "rmo is not materialized...computing it "
#         except Exception, e:
#             print "other execption occured..aborting ", str(e)
#             return

    if reload:
        if vu.check_model_exists(cls_name):
            return vu.import_model(cls_name)

    m_cls = schema.create_dbschema_for_rmo(
        session, rmo, tbl_name, cls_name, indexes, cls_fn, pks)
    cols = vam.getColNames(session, rmo)
    m_tbl = vam.get_base_table(session, m_cls)
    if populate_table is True:
        session.execute(m_tbl.insert().from_select(cols, rmo))
    session.commit()
    write_rmo_def(session, m_cls, cls_name,  tbl_name, cls_fn)
    return m_cls


def write_rmo_def(session=None, rmo=None, cls_name=None, tbl_name=None, cls_fn=None):
    """
    Write class definition of an rmo in file.

    Args:
         rmo: the input rmo
         cls_name: the class name for the rmo
         tbl_name : The table which represents the storage
         cls_fn : the output file

    Usage:


    Comment:
         Will create file 'foo.py' with class name foo_cls, table tbl_name, and  attributes from rmo.

     Issues:
         if primary key is not defined, can fail

    """
    if cls_fn is None:
        cls_fn = cls_name + "_model.py"

    rmo = vam.buildstmt(session, rmo)
    metadata = MetaData()
    attrs = vam.getColNames(session, rmo)
    cols = attrs
    types = [getattr(rmo.c, col).type for col in cols]
    primary_keys = [getattr(rmo.c, col).primary_key for col in cols]
    col_desc = ""
    for col, ctype, pk in zip(attrs, types, primary_keys):
        if hasattr(ctype, 'geometry_type'):
            geom_type_formatting = ctype.__repr__().capitalize()
            col_desc = col_desc + \
                Template("""\t${col} = Column(${geom_type_formatting}, primary_key=${pk})\n""").substitute(
                    locals())
        else:
            col_desc = col_desc + \
                Template("""\t${col} = Column(${ctype}, primary_key=${pk})\n""").substitute(
                    locals())

    rmo_desc = Template(
        """from header import *\nclass ${cls_name}(Base):\n\t__tablename__='${tbl_name}'\n${col_desc}\n""").substitute(locals())
    with open(cls_fn, 'w+') as fh:
        fh.write(rmo_desc)


def scan(session=None, rmo=None):
    assert(session is not None)
    return session.query(rmo).all()


def build_iter(session=None, rmo=None):
    """
    returns an iterator over the rmo. 
    To apply iterator to a function f:

    mynewrmo = sc.proj(session, rmo, ['attr1', 'attr2']


    from itertools import chain, imap
    import  collections
    rmo_iter = build_iter(session, rmo)
    collections.deque(imap(f, rmo_iter))

    Additional notes: 
    To build a functor with partial bindings:
    add_models_func = (lambda ri=None: myfunc(ri, attr, map)) -- assuming 5 is the other
    argument

    def myfunc(ri, attr, map):
         map(to_shape(ri._asdict()[attr]))


    """
    class rec_iter:
        def __init__(self, session, rmo):
            self.result_set = scan(session, rmo)

        def __iter__(self):
            for row in self.result_set:
                yield row

    ri = rec_iter(session, rmo)
    return ri


def to_dict(namedtuplelist, key_attr):
    '''
    converts the output of an aggregate to a dictionary
    TODO: raise error on key conflict
    '''
    resdict = {}
    for pair in namedtuplelist:
        resdict[pair._asdict()[key_attr]] = pair
    return resdict


def to_xml(namedtuplelist):
    '''
    converts a rows as xml. column/attribute name is the tag. 
    type is ignored for now. not recommended for super large 
    tables. Not recommended for tables with columns that are complex types (geom, xml, ltree)
    '''

    xml_string = '''<rmo>\n'''

    for rec in namedtuplelist:
        rec_xml = '''<rec>'''
        for f in rec._fields:  # for each field of rec
            v = getattr(rec, f)  # value of field f
            rec_xml = rec_xml + '<' + f + '>' + str(v) + '</' + f + '>'
        rec_xml = rec_xml + "</rec>\n"
        xml_string = xml_string + rec_xml
    xml_string = xml_string + '''</rmo>'''
    return xml_string


def build_tabulate(session, stmt, tablefmt="simple"):
    """

    """
    cols = vam.getColNames(session, stmt)
    res = session.query(stmt).all()
    rows = []
    for r in res:
        row = []
        for col in cols:
            row.append(r._asdict()[col])
        rows.append(row)
    return [cols, tabulate(rows, headers=cols, tablefmt=tablefmt)]


def copy_to_file(session=None, rmo=None, file_path=None, delimiter=',', header=None):
    """
    copy the content of rmo to file_path 

    Args:
    delimiter:

    header: without newline
    """
    rmo = vam.buildstmt(session, rmo)
    for col in rmo.c:
        if hasattr(col.type, 'geometry_type'):
            stmt = session.query(
                rmo, (cast(ST_AsText(ST_GeomFromWKB(col)), String))).subquery()
            rmo = vam.buildstmt(session, stmt)
    # TODO Need something like ST_AsText(  ST_GeomFromWKB( : select pid, ST_GeomFromWKB(origin_coord) from f1;
    delimiter_label = None
    if delimiter == ',':
        delimiter_label = 'comma'
    if delimiter == '|':
        delimiter_label = 'pipe'
    if delimiter == ' ':
        delimiter_label = 'space'
    assert delimiter_label is not None

    flags = {'format': 'csv', 'header': False, 'delimiter': delimiter}
    with open(file_path, 'w') as fh:
        if header:
            fh.write(header + "\n")
        copy_to(rmo, fh, session.connection().engine, **flags)


def copy_to_file_bevy_iter(session=None, rmo_iter=None, file_path=None, delimiter=',', header=None):
    """
    copy the content of rmo to file_path 

    Args:
    delimiter:

    header: without newline
    """

    delimiter_label = None
    if delimiter == ',':
        delimiter_label = 'comma'
    if delimiter == '|':
        delimiter_label = 'pipe'
    if delimiter == ' ':
        delimiter_label = 'space'
    assert delimiter_label is not None

    flags = {'format': 'csv', 'header': False, 'delimiter': delimiter}
    with open(file_path, 'w') as fh:
        if header:
            fh.write(header + "\n")
        for rmo in rmo_iter:
            rmo = vam.buildstmt(session, rmo)
            copy_to(rmo, fh, session.connection().engine, **flags)


def scan_singleton(session=None, rmo=None):
    '''
    return a single row 
    '''
    return session.query(rmo).one()
