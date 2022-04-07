import getpass
import os
import tempfile
from string import Template


#from utils import timing

from ..common import xmlutils as xu
from ..common import metadata_utils as mu
from ..common import utilities
from ..controller import pgsa_utils as pgu
from . import edi_config_utils as ecu
from . import load_file_impl as lfi
from . import build_csv_metadata as bcm

module_dir = os.path.dirname(os.path.realpath(__file__))
user = getpass.getuser()
port = None
conn = None
cursor = None
work_dir = None


def init_connection(dbdesc, wdir=None):
    global cursor
    global port
    global work_dir
    global conn
    global user

    host = pgu.get_db_host(dbdesc, wdir)
    port = pgu.get_db_port(dbdesc, wdir)

    assert(port != None)
    conn = pgu.get_conn_handle(host, port, user)
    cursor = conn.cursor()
    if wdir is None:
        work_dir = tempfile.mkdtemp()
    else:
        work_dir = wdir
    #cursor.execute("create extension if not exists oracle_fdw");
    #cursor.execute("create extension if not exists postgres_fdw")
    #cursor.execute("create extension if not exists quantile");
    return conn


def build_csv_metadata(cfg_xml):
    if xu.check_if_xml_tree(cfg_xml):
        croot = cfg_xml
    else:
        croot = ecu.read_config_file(cfg_xml)
    all_files = ecu.get_files(croot)
    if all_files is None:
        return
    for fe in all_files:
        metadata = fe['metadata']
        location = fe['location']
        model = fe['model_name']
        delimiter = fe['delimiter']
        if model is not None:
            bcm.build_csv_metadata(
                model_name=model, file_path=location, metadata_path=metadata, delimiter=delimiter)


def build_oracle_metadata(cfg_xml):
    '''
    create metadata file for all oracle tables. The resulting metadata file  is incomplete as it is 
    missing primary key/foreign key tables. 
    The output is in model_name.md
    '''
    if xu.check_if_xml_tree(cfg_xml):
        croot = cfg_xml
    else:
        croot = ecu.read_config_file(cfg_xml)

    all_rds = ecu.get_data_servers(croot, 'oracle')  # rds-->remote dataserver
    if all_rds is None:
        return
    for rd in all_rds.keys():
        dbname = rd  # actually should be the sfxlogin alias
        dbuser = all_rds[dbname].user
        url = all_rds[dbname].url
        passw = all_rds[dbname].password
        pgu.create_remote_oracledb_server(
            user, cursor, dbname, url, dbuser, passw)
        conn.commit()

    remote_tbls = ecu.get_remote_tables(croot, all_rds)
    if remote_tbls is None:
        return
    for remote_tbl in remote_tbls:
        dbname = remote_tbl[0]
        dbuser = all_rds[dbname].user
        url = all_rds[dbname].url
        passw = all_rds[dbname].password
        tbl_schema = remote_tbl[1]
        tbl_name = remote_tbl[2]
        model_name = remote_tbl[5]
        pgu.create_oratbl_metadata(cursor, work_dir, port, tbl_schema,
                                   tbl_name, dbname, all_rds[dbname], model_name=model_name)


def build_postgres_tables_metadata(cfg_xml):
    """
       Remote tables in oracle databases are copied locally.
    """
    croot = ecu.read_config_file(cfg_xml)
    all_rds = ecu.get_data_servers(
        croot, 'postgres')  # rds-->remote dataserver

    for rd in all_rds.keys():
        dbname = rd  # actually should be the sfxlogin alias
        dbuser = all_rds[dbname].user
        url = all_rds[dbname].url
        passw = all_rds[dbname].password
        database = all_rds[dbname].database
        pgu.create_remote_postgres_server(user=user, cursor=cursor, remotedb_name=dbname, remotedb_url=url,
                                          remotedb_database=database, remotedb_user=dbuser, remotedb_password=passw)
        conn.commit()

    remote_tbls = ecu.get_remote_tables(croot, all_rds)
    if remote_tbls is None:
        return
    for remote_tbl in remote_tbls:
        dbname = remote_tbl[0]
        dbuser = all_rds[dbname].user
        url = all_rds[dbname].url
        passw = all_rds[dbname].password
        tbl_schema = remote_tbl[1]
        tbl_name = remote_tbl[2]
        tbl_database = remote_tbl[4]
        pgu.build_postgres_table_metadata(
            cursor, work_dir, port, tbl_database, tbl_schema, tbl_name, dbname, all_rds[dbname])


def build_shptbl_metadata(cfg_xml):
    """
       build metadata from shapefiles
    """
    croot = ecu.read_config_file(cfg_xml)
    if xu.check_if_xml_tree(cfg_xml):
        croot = cfg_xml
    else:
        # TODO: too much replication; refactor;
        croot = ecu.read_config_file(cfg_xml)
    all_files = ecu.get_shape_files(croot)
    if all_files is None:
        return
    for fe in all_files:
        metadata = fe['metadata']
        location = fe['location']

        model = fe['model_name']
        delimiter = fe['delimiter']
        if model is not None:
            pgu.build_shp_metadata(
                work_dir=work_dir, model_name=model, shp_loc=location, metadata_path=metadata)


def load_postgres_tables(cfg_xml, commit=True, make_local_copy=False):
    """
       Remote tables in oracle databases are copied locally.
    """
    croot = ecu.read_config_file(cfg_xml)
    all_rds = ecu.get_data_servers(
        croot, 'postgres')  # rds-->remote dataserver

    for rd in all_rds.keys():
        dbname = rd  # actually should be the sfxlogin alias
        dbuser = all_rds[dbname].user
        url = all_rds[dbname].url
        passw = all_rds[dbname].password
        database = all_rds[dbname].database
        pgu.create_remote_postgres_server(user=user, cursor=cursor, remotedb_name=dbname, remotedb_url=url,
                                          remotedb_database=database, remotedb_user=dbuser, remotedb_password=passw)
        conn.commit()

    remote_tbls = ecu.get_remote_tables(croot, all_rds)
    if remote_tbls is None:
        return
    for remote_tbl in remote_tbls:
        dbname = remote_tbl[0]
        dbuser = all_rds[dbname].user
        url = all_rds[dbname].url
        passw = all_rds[dbname].password
        tbl_schema = remote_tbl[1]
        tbl_name = remote_tbl[2]
        tbl_database = remote_tbl[4]
        pgu.create_pg_fdw_table(
            cursor, work_dir, port, tbl_database, tbl_schema, tbl_name, dbname, all_rds[dbname])
        if make_local_copy:
            pgu.copy_remote_table_to_local(port, cursor, tbl_name)
        if commit:
            conn.commit()


def create_fdw_oracle_tables(cfg_xml, commit=None, load=False):
    """
       Remote tables in oracle databases are copied locally.
    """
    croot = ecu.read_config_file(cfg_xml)
    all_rds = ecu.get_data_servers(croot, 'oracle')  # rds-->remote dataserver

    for rd in all_rds.keys():
        dbname = rd  # actually should be the sfxlogin alias
        dbuser = all_rds[dbname].user
        url = all_rds[dbname].url
        passw = all_rds[dbname].password
        pgu.create_remote_oracledb_server(
            user, cursor, dbname, url, dbuser, passw)
        conn.commit()

    remote_tbls = ecu.get_remote_tables(croot, all_rds)
    if remote_tbls is None:
        return
    for remote_tbl in remote_tbls:
        dbname = remote_tbl[0]
        dbuser = all_rds[dbname].user
        url = all_rds[dbname].url
        passw = all_rds[dbname].password
        # all oracle schema are referenced in upper letter
        tbl_schema = remote_tbl[1].upper()
        # all oracle tables are referenced in upper letter
        tbl_name = remote_tbl[2].upper()

        pgu.create_fdw_table(cursor, work_dir, port,
                             tbl_schema, tbl_name, dbname, all_rds[dbname])
        if load:
            pgu.copy_remote_table_to_local(port, cursor, tbl_name)
        if commit:
            conn.commit()


def load_oracle_tables(cfg_xml, commit=None):
    """
       Remote tables in oracle databases are copied locally.
    """
    create_fdw_oracle_tables(cfg_xml, commit=None, load=True)
    conn.commit()

# @timing: TODO


def load_csv_file(metadata_xml, locations, wd="./", dbport=None, pgdb='postgres', model_name=None, schema='public', delimiter=None, strict_formatted=False, has_header=False, crop_head=None):
    assert type(locations) is list

    assert delimiter is not None
    global port
    if dbport is None:
        dbport = port
    if xu.check_if_xml_tree(metadata_xml):
        metadata_root = metadata_xml
    else:
        metadata_root = ecu.read_config_file(metadata_xml)

    if model_name is not None:
        mu.set_model_name(metadata_root, model_name)

    # removing creation of table because it should be done explicitly
    #lfi.create_table_from_metadata(metadata_root, wd=wd, port=dbport, pgdb=pgdb, schema=schema)
    for location in locations:
        if utilities.is_url(location):
            lfi.ingest_from_url(metadata_root,  cursor, location,  wd, dbport, pgdb,
                                schema=schema, strict_formatted=strict_formatted, delimiter=delimiter, has_header=has_header, crop_head=crop_head)
        else:
            lfi.ingest_from_file(metadata_root,  cursor, os.path.dirname(location), os.path.basename(location), wd, dbport, pgdb,
                                 schema=schema, strict_formatted=strict_formatted, delimiter=delimiter, has_header=has_header, crop_head=crop_head)
    conn.commit()


def create_tables(cfg_xml, wd=None, dbport=None, pgdb='postgres', schema='public'):
    '''
    create schema according to metadata for files; -- we don't need file element only the metadata element and 
    the model name
    '''
    global port
    assert(port is not None)

    if dbport is None:
        dbport = port

    if wd is None:
        global work_dir
        wd = work_dir
    if xu.check_if_xml_tree(cfg_xml):
        croot = cfg_xml
    else:
        croot = ecu.read_config_file(cfg_xml)

    all_files = ecu.get_files(croot)
    if all_files is None:
        return

    for fe in all_files:
        metadata = fe['metadata']

        model_name = fe.get('model_name')
        schema = fe.get('schema')
        if schema is None:
            schema = 'public'
        metadata_root = mu.read_metadata(metadata)
        if model_name is not None:
            mu.set_model_name(metadata_root, model_name)
        lfi.create_table_from_metadata(
            metadata_root, wd=work_dir, port=dbport, pgdb=pgdb, schema=schema)
    conn.commit()


def create_table_from_metadata(metadata_root, wd=None, pgdb='postgres'):
    lfi.create_table_from_metadata(metadata_root, wd=wd, port=port, pgdb=pgdb)
    conn.commit()


def load_raster_file(metadata, locations):
    global port
    assert type(locations) is list
    metadata_root = mu.read_metadata(metadata)
    model_name = mu.get_model_name(metadata_root)
    col_type = mu.get_columns_and_type(metadata_root)
    for location in locations:
        lfi.load_raster_file(port, location)
    conn.commit()


def create_shape_table_from_file(rasterFile, model_name=None):
    global port
    lfi.create_shape_table_from_file(port, rasterFile, model_name)
    conn.commit()


def create_raster_table_from_file(rasterFile, model_name=None):
    global port
    lfi.create_raster_table_from_file(port, rasterFile, model_name)
    conn.commit()


def load_shape_data_from_file(metadata, locations):
    global port
    assert type(locations) is list
    metadata_root = mu.read_metadata(metadata)
    model_name = mu.get_model_name(metadata_root)
    for location in locations:
        lfi.load_shape_data_from_file(port, location, model_name=model_name)
    conn.commit()


def load_raster_data_from_file(metadata, locations):
    global port
    assert type(locations) is list
    metadata_root = mu.read_metadata(metadata)
    model_name = mu.get_model_name(metadata_root)
    for location in locations:
        lfi.load_raster_data_from_file(port, location, model_name=model_name)
    conn.commit()


def load_files(cfg_xml, pgdb=None):
    global port
    assert(port is not None)
    if xu.check_if_xml_tree(cfg_xml):
        croot = cfg_xml
    else:
        croot = ecu.read_config_file(cfg_xml)

    all_files = ecu.get_files(croot)

    if all_files is None:
        return

    for fe in all_files:
        print("fe = ", fe)
        metadata = fe['metadata']
        location = fe['location']
        model_name = fe.get('model_name')
        schema = fe.get('schema')
        delimiter = fe.get('delimiter')
        strict_formatted = fe.get('strict_formatted')
        has_header = fe.get('has_header')
        crop_head = fe.get('crop_head')
        print("crop head = ", crop_head)

        if schema is None:
            schema = 'public'
        metadata_root = mu.read_metadata(metadata)

        if model_name is not None:
            mu.set_model_name(metadata_root, model_name)
        #load_file(metadata, location,work_dir,port, pgdb, model_name=model_name, schema=schema)
        #lfi.create_table_from_metadata(metadata, work_dir)

        if location is not None:
            lfi.ingest_from_file(metadata_cfg=metadata_root, cursor=cursor, ddir=os.path.dirname(location), fn=os.path.basename(
                location), work_dir=work_dir, port=port, delimiter=delimiter, strict_formatted=strict_formatted, crop_head=crop_head, has_header=has_header)
        if fe['locations'] is not None:
            file_locations = xu.get_value_elems(fe['locations'], 'location')
            for location in file_locations:
                lfi.ingest_from_file(metadata_cfg=metadata_root,  cursor=cursor, dir=os.path.dirname(location), fn=os.path.basename(
                    location), work_dir=work_dir, port=port, delimiter=delimiter, strict_formatted=strict_formatted)
        conn.commit()


def close_session():
    conn.commit()
    conn.close()

# init_connection("mydb")
# load_oracle_tables("ed.xml")
# load_files("ed.xml")
