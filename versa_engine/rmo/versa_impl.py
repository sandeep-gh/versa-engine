from string import Template
import subprocess

from ..controller.appconfig import AppConfig
from ..controller import pgsa_utils as pgu
from ..common import metadata_utils as mu
from ..common import xmlutils as xu
from ..edi import dicex_edi_impl as dei
from ..edi import edi_config_utils as ecu
from ..edi import load_file_impl as lfi
from . import utils as vu
#import versa_api as vapi
# provides port, session, sqlalchemy resources, etc.
from .versa_header import *
# TODO: fix the work dir stuff'


def oracle_init(user="WISDM_DEV", password="change1t"):
    #global conn_str
    #global engine
    #global Session
    #global session
    #conn_str = Template("oracle+cx_oracle://${user}:${password}@noldor-db.vbi.vt.edu:1521/ndssl.bioinformatics.vt.edu").substitute(locals())
    conn_str = Template(
        "oracle+cx_oracle://${user}:${password}@noldor-db.vbi.vt.edu:1521/ndssl").substitute(locals())
    engine = create_engine(conn_str)
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    return session


def init(dbdesc, run_dir=None):
    global port
    global conn_str
    global engine
    global Session
    global session

    wdir = run_dir
    port = pgu.get_db_port(dbdesc, wdir)
    dbhost = pgu.get_db_host(dbdesc, wdir)
    work_dir = pgu.get_work_dir(dbdesc, wdir)
    dbadapter = dei.init_connection(dbdesc, wdir)
    #conn_str='postgresql+psycopg2://'+str(dbhost)+':' + str(port) + '/postgres?charset=utf8'
    conn_str = 'postgresql+psycopg2://' + \
        str(dbhost)+':' + str(port) + '/postgres'
    engine = create_engine(conn_str)
    Session = sessionmaker()
    Session.configure(bind=engine)
    dbsession = Session()
    return [dbadapter, dbsession]


def append_import_header(tbl_name, wd):
    #####################
    # add to runner
    ####################
    work_dir = wd
    import_header = open(f'{work_dir}/.x.py', 'a')
    print("from ", tbl_name + '_model import *', file=import_header)
    import_header.close()
    cmd_str = f'sort -n {work_dir}/.x.py | uniq > {work_dir}/import_header.py'

    print("import header cmd = ", cmd_str)
    print("work_dir = ", work_dir)
    subprocess.call(cmd_str, shell=True)


def build_orm_from_metadata(metadata_root, base_tbl_name=None, create_table=False, force_create_model=True, location=None, class_decorator="", work_dir="./"):

    model_name = mu.get_model_name(metadata_root)

    if create_table:  # also create table from the metadata
        dei.create_table_from_metadata(metadata_root, wd=work_dir)

    primary_keys = mu.get_primary_keys(metadata_root)
    foreign_keys = mu.get_foreign_keys(metadata_root)
    fh = open(work_dir.rstrip() + "/" + model_name.strip() + ".schema", 'w+')
    #mfh = open(work_dir.rstrip() + "/"+ model_name.strip() + ".mirror_schema", 'w+')
    foreign_keys_string = ""
    all_columns = mu.get_columns_and_type(metadata_root)

    for cname, ctype in all_columns:
        if cname and ctype:
            if cname in foreign_keys:
                foreign_keys_string = foreign_keys_string + '\\\'' + cname + "\\\',"
                cname = 'f_' + cname

            print(cname, " ", ctype, file=fh)  # TODO: Check if appending
            #print >>mfh, 'm_'+cname," ", ctype

    fh.close()
    # mfh.close()
    foreign_keys_string = '['+foreign_keys_string[:-1] + ']'

    tbl_name = model_name.strip().lower()
    clone_tbl_name = tbl_name + '_mirror'
    clone_model_name = model_name + '_mirror'
    primary_key = vu.build_primary_key(primary_keys, foreign_keys)
    clone_primary_key = vu.build_clone_primary_key(primary_keys, foreign_keys)
    setenv_path = AppConfig.get_setevn_path()
    if base_tbl_name is not None:
        # essentially [create table tbl_name as select * from base_tbl_name]
        create_pg_tbl_cmd = Template(
            ". ${setenv_path}; awk -f ${module_dir}/create_view.awk ${work_dir}/${model_name}.schema | sed \'s/__TBL__/\'$base_tbl_name\'/g\' | sed \'s/__VIEW__/\'${tbl_name}\'/g\' | psql -p $port postgres  >> ${work_dir}/psql_out")
        a = locals()
        b = globals()
        a.update(b)
        create_pg_tbl_cmd_str = create_pg_tbl_cmd.substitute(a)
        subprocess.call(create_pg_tbl_cmd_str, shell=True)
    # add import header.py stmt
    subprocess.call("echo \"from versa_engine.rmo.header import * \" > " +
                    work_dir + "/" + model_name+"_model.py", shell=True)

    #create orm###
    wd = work_dir
    create_orm_cmd = Template("awk -f ${module_dir}/create_orm_from_schema.awk ${wd}/${model_name}.schema  | sed \'s/__MODEL__/\'${model_name}\'/\' | sed \'s/__TBL__/\'${model_name}\'/\' | sed \'s/__PKEY_STRING__/\'${primary_key}\'/\' | sed \'s/__CLASS_DECORATOR__/\'${class_decorator}\'/\'| sed \'s/__FOREIGN_KEYS_STRING__/\'${foreign_keys_string}\'/\' >> ${wd}/${model_name}_model.py")
    a = locals()
    b = globals()
    a.update(b)
    create_orm_cmd_str = create_orm_cmd.substitute(a)
    print("create orm cmd = ", create_orm_cmd_str)
    print("work_dir = ", work_dir)
    # print create_orm_cmd_str
    subprocess.call(create_orm_cmd_str, shell=True)
    append_import_header(tbl_name, work_dir)

    # build clone
    # phasing out cloned view-- we no longer need that
    # create_clone_pg_tbl_cmd= Template("awk -f ${module_dir}/create_cloned_view.awk ${work_dir}/${model_name}.mirror_schema | sed \'s/__TBL__/\'$tbl_name\'/g\' | sed \'s/__VIEW__/\'${clone_tbl_name}\'/g\' | psql -p $port postgres  >> ${work_dir}/psql_out")
#     create_clone_pg_tbl_str = create_clone_pg_tbl_cmd.substitute(a)
#     print create_clone_pg_tbl_str
#     subprocess.call(create_clone_pg_tbl_str, shell=True)


#     create_clone_orm_template = Template("awk -f ${module_dir}/create_orm_from_schema_mirror.awk $work_dir/${model_name}.mirror_schema | sed \'s/__MODEL__/\'${clone_model_name}\'/\' |  sed \'s/__CLASS_DECORATOR__/\'${class_decorator}\'/\' | sed \'s/__TBL__/\'$clone_tbl_name\'/\' | sed \'s/__PKEY_STRING__/\'${clone_primary_key}\'/\' >> ${model_name}_model.py")
#     create_clone_orm_str=create_clone_orm_template.substitute(a)
#     subprocess.call(create_clone_orm_str, shell=True)
    #build_indexes_for_clone(clone_tbl_name, metadata_node.getElementsByTagName('primarykey')[0], foreign_keys)

    if location is not None:
        # this is outdata now. need fixing.
        dei.load_csv_file(metadata_root, location)


def build_only_orm_from_metadata(metadata_root, base_tbl_name=None, create_table=False, force_create_model=True, location=None, class_decorator="", work_dir="./"):
    model_name = mu.get_model_name(metadata_root)
    if create_table:
        dei.create_table_from_metadata(metadata_root, wd=work_dir)

    primary_keys = mu.get_primary_keys(metadata_root)
    foreign_keys = mu.get_foreign_keys(metadata_root)
    fh = open(work_dir.rstrip() + "/" + model_name.strip() + ".schema", 'w+')
    mfh = open(work_dir.rstrip() + "/" +
               model_name.strip() + ".mirror_schema", 'w+')
    foreign_keys_string = ""
    all_columns = mu.get_columns_and_type(metadata_root)
    for cname, ctype in all_columns:
        if cname and ctype:
            if cname in foreign_keys:
                foreign_keys_string = foreign_keys_string + '\\\'' + cname + "\\\',"
                cname = 'f_' + cname

            print(cname, " ", ctype, file=fh)
            print('m_'+cname, " ", ctype, file=mfh)

    fh.close()
    mfh.close()
    foreign_keys_string = '['+foreign_keys_string[:-1] + ']'

    tbl_name = model_name.strip().lower()
    clone_tbl_name = tbl_name + '_mirror'
    clone_model_name = model_name + '_mirror'
    primary_key = vu.build_primary_key(primary_keys, foreign_keys)
    clone_primary_key = vu.build_clone_primary_key(primary_keys, foreign_keys)

    #subprocess.call("echo \"from header import * \" > "+ model_name+"_model.py", shell=True)

    #create orm###
    wd = work_dir
    create_orm_cmd = Template("awk -f ${module_dir}/create_orm_from_schema.awk ${wd}/${model_name}.schema  | sed \'s/__MODEL__/\'${model_name}\'/\' | sed \'s/__TBL__/\'${model_name}\'/\' | sed \'s/__PKEY_STRING__/\'${primary_key}\'/\' | sed \'s/__CLASS_DECORATOR__/\'${class_decorator}\'/\'| sed \'s/__FOREIGN_KEYS_STRING__/\'${foreign_keys_string}\'/\' >> ${wd}/${model_name}_model.py")
    a = locals()
    b = globals()
    a.update(b)
    create_orm_cmd_str = create_orm_cmd.substitute(a)

    subprocess.call(create_orm_cmd_str, shell=True)
    append_import_header(tbl_name, work_dir)


def build_only_orms(cfg_xml, force_create_model=False):
    '''
    For the tables are already present -- create orm
    '''
    if xu.check_if_xml_tree(cfg_xml):
        croot = cfg_xml
    else:
        croot = ecu.read_config_file(cfg_xml)

    all_files = ecu.get_files(croot)
    if all_files is None:
        return

    for fe in all_files:
        metadata = fe['metadata']
        model_name = fe['model']
        base_tbl = fe['base_tbl']
        metadata_root = mu.read_metadata(metadata)
        if model_name is None:
            model_name = mu.get_model_name(metadata_root)
        else:
            mu.set_model_name(metadata_root, model_name)

        if base_tbl is None:
            base_tbl = model_name

        if vu.check_model_exists(model_name, work_dir) and not force_create_model:
            pass
        else:
            build_only_orm_from_metadata(
                metadata_root, force_create_model=force_create_model, work_dir=work_dir)

# another cryptic name
# this is the entry point for dicex_edi
# currently only file loading is supported -- support for remote tables and gp-pipeline is not that hard


def build_orms(cfg_xml, work_dir="./", force_create_model=False, depth=0):
    '''
    input: takes a edconfig xml
    outcome: loads the data in the config file
    '''
    if depth == 20:
        print("build orm possibly stuck in recursive loop.... quitting")
        return

    if xu.check_if_xml_tree(cfg_xml):
        croot = cfg_xml
    else:
        croot = ecu.read_config_file(cfg_xml)
    all_models = build_orm_files(
        croot, force_create_model=force_create_model, work_dir=work_dir)
    if xu.has_key(croot, 'edconfig', path_prefix='./'):
        edcfg_elems = xu.get_elems(croot, 'edconfig', path_prefix='./')
        for edcfg_elem in edcfg_elems:
            # recursively call edcfg on nested edcfgs
            more_models = build_orms(
                edcfg_elem, work_dir, force_create_model=force_create_model, depth=depth+1)
        for x in more_models:  # awful as awful does
            all_models.append(x)
    return all_models


def build_orm_files(cfg_xml, datasets=None, force_create_model=False, work_dir="./"):
    if xu.check_if_xml_tree(cfg_xml):
        croot = cfg_xml
    else:
        croot = ecu.read_config_file(cfg_xml)

    if not xu.has_key(croot, 'files'):
        return None

    files_elem = xu.get_elems(croot, 'files', path_prefix='./', uniq=True)
    all_files = ecu.get_files(croot)
    if all_files is None:
        return None

    all_models = []
    for fe in all_files:
        metadata = fe['metadata']
        location = fe['location']
        locations = fe['locations']
        model_name = fe['model_name']
        strict_formatted = fe['strict_formatted']
        delimiter = fe['delimiter']
        has_header = fe['has_header']
        crop_head = fe['crop_head']

        # if datasets is None then False
        # if model_name not in datasets then false
        if datasets is not None and model_name not in datasets:
            continue

        if strict_formatted is None:
            strict_formatted = False
        # this is band-aid patch fix for
        file_locations = []
        if locations is not None:
            file_locations = xu.get_value_elems(fe['locations'], 'location')
        if location is not None:
            file_locations.append(location)
        if fe['filetype'] is not None:
            filetype = fe['filetype']
        metadata_root = mu.read_metadata(metadata)
        if model_name is None:
            model_name = mu.get_model_name(metadata_root)
        else:
            mu.set_model_name(metadata_root, model_name)

        all_models.append(model_name)

        if vu.check_model_exists(model_name, work_dir) and not force_create_model:
            pass
        else:
            if filetype == 'csv':
                dei.create_table_from_metadata(metadata_root, wd=work_dir)
                dei.load_csv_file(metadata_root, file_locations, wd=work_dir, model_name=model_name,
                                  strict_formatted=strict_formatted, delimiter=delimiter, has_header=has_header, crop_head=crop_head)
            elif filetype == 'raster':
                dei.create_raster_table_from_file(
                    file_locations[0], model_name=model_name)
                dei.load_raster_data_from_file(metadata_root, file_locations)
            elif filetype == 'shape':
                dei.create_shape_table_from_file(
                    file_locations[0], model_name=model_name)
                dei.load_shape_data_from_file(metadata_root, file_locations)
            build_orm_from_metadata(
                metadata_root, force_create_model=force_create_model, work_dir=work_dir)
    print("all_models = ", all_models)
    return all_models


def build_orm_oracle_tables(cfg_xml, make_local_copy=False, force_create_model=False, work_dir="./"):
    '''
    load remote oracle tables and build orm
    '''

    # removing this line; we assume metadata is given
    # dei.build_oracle_metadata(cfg_xml)

    dei.create_fdw_oracle_tables(cfg_xml, load=make_local_copy, commit=True)
    croot = ecu.read_config_file(cfg_xml)
    all_rds = ecu.get_data_servers(croot, 'oracle')  # rds-->remote dataserver
    # passing xml as array is a really bad idea
    remote_tbls = ecu.get_remote_tables(croot, all_rds)
    if remote_tbls is not None:
        for remote_tbl in remote_tbls:

            # currently this option is ignored
            metadata = remote_tbl[3]
            # this is really fragile--when will i get time to fix it
            model_name = remote_tbl[-1]
            base_tbl_name = remote_tbl[2]
            if make_local_copy is False:
                # when we are not making a local copy but creating a fdw on it
                base_tbl_name = base_tbl_name + "_fdw"
            if metadata is None:
                metadata_root = mu.read_metadata(model_name + ".md")
            else:
                metadata_root = mu.read_metadata(metadata)
            mu.set_model_name(metadata_root, model_name)
            if vu.check_model_exists(model_name) and not force_create_model:
                pass
            else:
                build_orm_from_metadata(
                    metadata_root, base_tbl_name=base_tbl_name, work_dir=work_dir)
        return all_models

# def build_orm_gp_pipeline(cfg_xml):
#     import sidap_utils as su
#     croot=ecu.read_config_file(cfg_xml)
#     if  xu.has_key(croot, 'gp_pipeline', path_prefix='./'):
#         pipeline_log = xu.get_value_elem(croot, 'gp_pipeline/gp_pipeline_log')
#         pipeline_log_root = xu.read_file(pipeline_log)
#         mapping_root = None #use default mapping file
#         all_sios = xu.get_elems(croot, 'sio', path_prefix=".//gp_pipeline/sio_list/")
#         for sio in all_sios:
#             su.build_relational_mapping(sio.text, mapping_root, pipeline_log_root, work_dir)
#         pass
#     else:
#         pass

#     return


def build_orm_pg_tables(cfg_xml, force_create_model=False, make_local_copy=False, work_dir="./"):
    # creates foreign data wrapper
    dei.load_postgres_tables(cfg_xml, commit=True,
                             make_local_copy=make_local_copy)
    croot = ecu.read_config_file(cfg_xml)
    all_rds = ecu.get_data_servers(
        croot, 'postgres')  # rds-->remote dataserver
    if all_rds is None:
        return
    remote_tbls = ecu.get_remote_tables(croot, all_rds)
    if remote_tbls is None:
        return

    for remote_tbl in remote_tbls:
        metadata = remote_tbl[3]
        metadata_root = mu.read_metadata(metadata)
        model_name = mu.get_model_name(metadata_root)
        base_tbl_name = remote_tbl[2]
        if make_local_copy is False:
            base_tbl_name = base_tbl_name + "_fdw"

        if vu.check_model_exists(model_name) and not force_create_model:
            pass
        else:
            build_orm_from_metadata(
                metadata_root, create_table=False, base_tbl_name=base_tbl_name, work_dir=work_dir)


def wrap_file(dbsession=None, metadata_fn=None, data_fn=None):
    global work_dir
    port = pgu.get_db_port(dbsession)
    metadata_root = mu.read_metadata(metadata_fn)
    build_orm_from_metadata(metadata_root, create_table=True)
    lfi.ingest_from_file(metadata_file=metadata_fn, dir=os.path.dirname(
        data_fn), fn=os.path.basename(data_fn), wd=work_dir, port=port)


def export_tables_to_files(session=None, cfg_xml=None, export_dir=None, dry_run=False):
    '''

    Input:
    session: is a sqlalchemcy session object
    cfg_xml: is a list of remote tables (oracle or postgres), that is already loaded or has a fdw. 
    dry_run: skip exporting of data
    files_elem: xml files elem, i.e., the container for each file config
    Output: Export the tables to CSV files
    '''

    all_file_elems = []
    croot = ecu.read_config_file(cfg_xml)
    table_elems = xu.get_elems(croot, 'remote_table')
    if table_elems is None:
        return None
    for tbl in table_elems:
        tserver = xu.get_value_by_attr(tbl, 'server')
        tschema = xu.get_value_by_attr(tbl, 'schema')
        tname = xu.get_value_by_attr(tbl, 'name')
        model_name = tname  # default
        if xu.has_key(tbl, 'model_name'):
            model_name = xu.get_value_by_attr(tbl, 'model_name')
        metadata = xu.get_value_by_attr(tbl, 'metadata')
        assert metadata is not None
        model = None
        if not dry_run:
            model = vu.import_model(model_name)
        [export_loc, file_elem] = vapi.export_rmo(
            session, model, model_name, export_dir=export_dir, dry_run=dry_run)
        xu.update_elem_value(file_elem, 'metadata', metadata)
        #export_loc = export_dir + "/" + model_name + ".csv"
        #file_xml = Template('''<file><location>$export_loc</location><metadata>$metadata</metadata><model_name>$model_name</model_name><filetype><filetype>csv</filetype><strict_formatted>True</strict_formatted><delimiter>comma</delimiter></filetype></file>''').substitute(locals())
        all_file_elems.append(file_elem)
    return all_file_elems
    # TODO: figure out how to grow xml, snippet by snippet
