from string import Template
import os
import sys
import subprocess
import getpass
import socket
import imp
from datetime import datetime
import psycopg2 as pgsai

from common import utilities

# mpi_file_transfer_exec="/home/pgxxc/public/FastTransferUtils/mpiSendRecv_multiproc"
# mpi_file_transfer_exec="/home/sandeep/NDSSL-Software/dicex_data_ingestion/dbimg_save_and_resume/multipleProcSendRecv"
hostname = socket.gethostname()
module_dir = os.path.dirname(os.path.realpath(__file__))
dicex_basedir = module_dir + "/../"
################################
# Python String Template escapes
################################
LD_LIBRARY_PATH = "$LD_LIBRARY_PATH"
PATH = "$PATH"
#################################


def get_session_fn(dbdesc=None):
    assert(dbdesc is not None)
    return 'dbsession_'+dbdesc


def get_db_host(dbdesc=None, wdir=None):
    session_fn = get_session_fn(dbdesc)
    return utilities.get_attr_value_from_module(module_dir=wdir, module_name=session_fn, module_attr='server_ip')


def get_db_host_type(dbdesc=None, wdir="./"):
    session_fn = get_session_fn(dbdesc)
    return utilities.get_attr_value_from_module(module_dir=wdir, module_name=session_fn, module_attr='host_type')


def get_work_dir(dbdesc=None, wdir=None):
    session_fn = get_session_fn(dbdesc)
    return utilities.get_attr_value_from_module(module_dir=wdir, module_name=session_fn, module_attr='work_dir')


def get_db_port(dbdesc=None, wdir="./"):
    ''' 
       returns the port mentioned in dbsession_<dbdesc>
    '''
    session_fn = get_session_fn(dbdesc)
    
    return utilities.get_attr_value_from_module(module_dir=wdir, module_name=session_fn, module_attr='server_port')


def get_cluster_name(dbdesc=None, wdir=None):
    ''' 
       returns the port mentioned in dbsession_<dbdesc>
    '''
    assert dbdesc is not None
    session_fn = get_session_fn(dbdesc)
    return utilities.get_attr_value_from_module(module_dir=wdir, module_name=session_fn, module_attr='cluster_name')


def get_db_host(dbdesc=None, wdir=None):
    ''' 
       returns the port mentioned in dbsession_<dbdesc>
    '''
    assert dbdesc is not None
    session_fn = get_session_fn(dbdesc)
    return utilities.get_attr_value_from_module(module_dir=wdir, module_name=session_fn, module_attr='server_ip')


def get_frontend_proxy_port(dbdesc=None, wdir=None):
    '''
       returns the port mentioned in dbsession_<dbdesc>
    '''
    assert dbdesc is not None
    session_fn = get_session_fn(dbdesc)
    return utilities.get_attr_value_from_module(module_dir=wdir, module_name=session_fn, module_attr='frontend')


def get_db_data_home(dbdesc=None, user=None, port=None, wdir=None):  # why is there dbdesc here
    assert(dbdesc is not None)
    assert(user is not None)
    if port is None:
        port = get_db_port(dbdesc, wdir=wdir)
    # TODO: the base needs to be a config parameter
    return Template("/home/kabira/var/${user}/pgsql_sa_${port}/").substitute(locals()) #TODO: the base directory should come from system_config_file


def get_pgpid(dbdesc, wdir=None):
    session_fn = get_session_fn(dbdesc)
    return utilities.get_attr_value_from_module(module_dir=wdir, module_name=session_fn, module_attr='pgpid')


def rcp_db_img(user, dbdesc=None, port=None,  target_node="sfx007"):
    assert(user is not None)
    assert(port is not None)
    db_data_tar_fn = Template(
        "/localscratch/${user}/pgsql_sa_${port}/data.tar").substitute(locals())
    db_data_tar_remote_fn = Template(
        "/localscratch/${user}/DBBackups/${dbdesc}.tar").substitute(locals())
    a = locals()
    b = globals()
    a.update(b)
    rcp_img_cmd_str = Template(
        "mkdir /localscratch/${user}/pgsql_sa_${port}; . /etc/profile.d/modules.sh; module load mpi/mvapich2/gcc/64/1.7; mpirun -np 8 --host ${target_node},${target_node},${target_node},${target_node},${hostname},${hostname},${hostname},${hostname} ${mpi_file_transfer_exec} ${db_data_tar_remote_fn} ${db_data_tar_fn} 262144").substitute(a)
    subprocess.call(rcp_img_cmd_str, shell=True)

    untar_cmd_str = Template(
        "cd /localscratch/${user}/pgsql_sa_${port}; tar xf data.tar").substitute(a)
    subprocess.call(untar_cmd_str, shell=True)


# shared_buffers="8GB"
# maintenance_work_mem="5GB"
# effective_cache_size="10GB"
# #checkpoint_segments="128" #no longer necessary in 9.6
# max_wal_size="8GB"
# checkpoint_timeout="30min"
# log_statement='all'
# max_connections=5  #should change for standard and largemem
#######################################
# TODO: change config per requirements
######################################
# shared_buffers=200GB
# maintenance_work_mem=200GB
# effective_cache_size=200GB
# checkpoint_segments=512
# checkpoint_timeout=60min

LD_LIBRARY_PATH = "${LD_LIBRARY_PATH}"

# def startdb(db_data_home, port=None, pgxxc=False, shared_buffers="8GB", maintenance_work_mem="5GB", work_mem="2GB", effective_cache_size="10GB", max_wal_size="8GB", checkpoint_timeout="30min", log_statement='all', max_connections="5"):


def create_oracle_fdw(sa_port, work_dir, dbuser, dbserver, schema, password):
    create_oracle_fdw_template_str = open(
        create_oracle_fdw.cmd.template, "r").read()
    a = locals()
    b = globals()
    a.update(b)
    create_oracle_fdw_cmd_str = create_oracle_fdw_template_str.substitute(
        a)  # ignore shell variables
    subprocess.call(create_oracle_fdw_cmd_str, shell=True)


def get_timestamp():
    currentHour = datetime.now().hour
    currentDay = datetime.now().day
    currentMonth = datetime.now().month
    currentYear = datetime.now().year
    # `currentYear`+"_" +`currentMonth`  +"_"+ `currentDay` + "_"+ `currentHour` #TODO
    timestamp = 'dummy'
    return timestamp


def build_load_module_cmd_str(modules):
    cmd_str = ". /etc/profile.d/modules.sh;"
    for mod in modules:
        cmd_str = cmd_str + "  module load " + mod + ";"
    return cmd_str


def get_conn_handle(host='localhost', port=5432, user=None):
    conn_string = Template(
        "host=${host} port='${port}' dbname='postgres'").substitute(locals())
    conn = pgsai.connect(conn_string)
    return conn
############################
# remote data ingestion
############################


def create_remote_oracledb_server(user=None, cursor=None, remotedb_name=None, remotedb_url=None, remotedb_user=None, remotedb_password=None):
    """opens a foreign data server for a remote oracle database

    :param conn: connection handle to the local database engine
    :param remote_server_name: an identifer/tag for the remote server
    :param remotedb_url: The full connection url 
    """
    try:
        cursor.execute(Template(
            "create server ${remotedb_name}_server foreign data wrapper oracle_fdw options (dbserver \'${remotedb_url}\')").substitute(locals()))
        cursor.execute(Template(
            "grant usage on foreign server ${remotedb_name}_server to  ${user}").substitute(locals()))

        cursor.execute(Template(
            "create user mapping for ${user} server ${remotedb_name}_server options (user '${remotedb_user}', password   '${remotedb_password}')").substitute(locals()))
    except:
        print("foreign server already created")


def create_remote_postgres_server(user=None, cursor=None, remotedb_name=None, remotedb_url=None, remotedb_database=None, remotedb_user=None, remotedb_password=None):
    """opens a foreign data server for a remote oracle database

    :param conn: connection handle to the local database engine
    :param remote_server_name: an identifer/tag for the remote server
    :param remotedb_url: The full connection url 
    """
    print(remotedb_database)
    try:
        cursor.execute(Template(
            "create server ${remotedb_name}_server foreign data wrapper postgres_fdw options (host \'${remotedb_url}\', dbname \'${remotedb_database}\')").substitute(locals()))
        cursor.execute(Template(
            "grant usage on foreign server ${remotedb_name}_server to  ${user}").substitute(locals()))
        cursor.execute(Template(
            "create user mapping for ${user} server ${remotedb_name}_server options (user '${remotedb_user}', password   '${remotedb_password}')").substitute(locals()))
    except Exception as e:
        print("exception in create postgres server")
        print(e)


def create_oratbl_metadata(cursor, work_dir, port, dbschema, tbl_name, dbname, dso, model_name=None):
    global module_dir
    dbuser = dso.user
    dbpass = dso.password
    dblocalid = dbname
    if model_name is None:
        model_name = tbl_name.lower()

    a = locals()
    b = globals()
    a.update(b)
    print("create_oratbl_metadata : ", model_name)
    build_oratbl_template = Template(
        '${module_dir}/build_oratbl_metadata.sh ${module_dir} ${work_dir} ${port} ${dblocalid} ${dbuser}  ${dbpass} ${dbschema} ${tbl_name} ${model_name} 2> ${work_dir}/build_oratbl_metadata.err')
    build_oratbl_cmd_str = build_oratbl_template.substitute(a)
    subprocess.call(build_oratbl_cmd_str, shell=True)
#     wrap_sql_str=open(work_dir+"/"+tbl_name+".wrap_sql", 'r').read()
#     cursor.execute(wrap_sql_str)


def build_postgres_table_metadata(work_dir=None, port=None, database=None, dbschema=None, tbl_name=None, dbname=None, dso=None):
    global module_dir
    dbuser = dso.user
    dbpass = dso.password
    dbhost = dso.url
    dbdb = database
    dblocalid = dbname
    a = locals()
    b = globals()
    a.update(b)
    create_md_template = Template(
        '${module_dir}/create_pgtbl_metadata.sh ${module_dir} ${work_dir} ${port} ${dbhost} ${dbuser}  ${dbpass} ${dbname} ${database} ${dbschema} ${tbl_name} 2> ${work_dir}/write_pg_fdw.err')
    create_md_str = create_md_template.substitute(a)
    subprocess.call(create_md_str, shell=True)


def build_shp_metadata(work_dir=None, shp_loc=None, model_name=None, metadata_path=None):
    global module_dir
    a = locals()
    b = globals()
    a.update(b)
    create_md_template = Template(
        '${dicex_basedir}/tools/build_shp_metadata.sh ${module_dir} ${work_dir} ${shp_loc} ${model_name} ${metadata_path} 2> ${work_dir}/build_shp_metadata.err')
    create_md_str = create_md_template.substitute(a)
    subprocess.call(create_md_str, shell=True)


def create_pg_fdw_table(cursor, work_dir, port, database, dbschema, tbl_name, dbname, dso):
    '''
    dbname is the  shadowfax name of the remote oracle database (ndssl, ndssldb, etc.)
    postgres has a notion of 
       - a database (ndsslgeo)
       - a schema adcw_7_2
    '''

    global module_dir
    dbuser = dso.user
    dbpass = dso.password
    dbhost = dso.url
    dbdb = dbschema  # essentially the adcw_7_2 part of adcw_7_2.adc_world_admin_2
    dblocalid = dbname
    a = locals()
    b = globals()
    a.update(b)
    create_fdw_template = Template(
        '${module_dir}/write_pg_fdw_on_pg_table.sh ${module_dir} ${work_dir} ${port} ${dbhost} ${dbuser}  ${dbpass} ${dbname} ${database} ${dbschema} ${tbl_name} 2> ${work_dir}/write_pg_fdw.err')
    create_fdw_str = create_fdw_template.substitute(a)
    subprocess.call(create_fdw_str, shell=True)
    wrap_sql_str = open(work_dir+"/"+tbl_name+".wrap_sql", 'r').read()
    print(wrap_sql_str)
    try:
        cursor.execute(wrap_sql_str)
    except:
        print("fdw creation failed. Most likely fdw for table ",
              tbl_name, " already exists; proceeding along")


def create_fdw_table(cursor, work_dir, port, dbschema, tbl_name, dbname, dso):
    """dso: data server instance which has connection and credential details
    """
    global module_dir
    dbuser = dso.user
    dbpass = dso.password
    dblocalid = dbname
    a = locals()
    b = globals()
    a.update(b)
    create_fdw_template = Template(
        '${module_dir}/write_pg_fdw_v2.sh ${module_dir} ${work_dir} ${port} ${dblocalid} ${dbuser}  ${dbpass} ${dbschema} ${tbl_name} > ${work_dir}/write_pg_fdw.out 2> ${work_dir}/write_pg_fdw.err')
    create_fdw_str = create_fdw_template.substitute(a)
    subprocess.call(create_fdw_str, shell=True)
    wrap_sql_str = open(work_dir+"/"+tbl_name+".wrap_sql", 'r').read()
    try:
        cursor.execute(wrap_sql_str)
    except:
        print("fdw creation failed. Most likely fdw for table ",
              tbl_name, " already exists; proceeding along")


def copy_remote_table_to_local(port, cursor, tbl_name):
    """
        Copies a remote file into the local database
    """
    create_tbl_cmd_template = Template(
        "create table ${tbl_name} as select * from ${tbl_name}_fdw")
    a = locals()
    b = globals()
    a.update(b)
    copy_tbl_cmd_str = create_tbl_cmd_template.substitute(a)
    cursor.execute(copy_tbl_cmd_str)
    #subprocess.call(copy_tbl_cmd_str, shell=True)


def build_index_on_column(tbl, column):
    create_index_cmd_template = Template(
        "psql -p ${port} postgres -c \"create index ${tbl}_${column} on ${tbl}(${column})\"")
    a = locals()
    b = globals()
    a.update(b)
    create_index_cmd_str = create_index_cmd_template.substitute(a)
    subprocess.call(create_index_cmd_str, shell=True)


def create_extension_postgis(dbport=None):
    assert (dbport is not None)
    cmd = Template(
        "psql -p ${dbport} postgres -c \"create extension postgis\"")
    print("postgis cmd  = ", cmd)
    subprocess.call(cmd.substitute(locals()), shell=True)
