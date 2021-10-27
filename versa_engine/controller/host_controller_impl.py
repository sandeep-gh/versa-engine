from string import Template
import subprocess
import os
from versa_engine.controller.appconfig import AppConfig
from versa_engine.controller import pgsa_utils as pgu


def initdb(db_data_home, port, allow_connections_from_subnet='True', db_cfg_param_val_dict=None, log_statement='none'):
    global work_dir
    global LD_LIBRARY_PATH
    assert(port is not None)
    assert(db_data_home is not None)
    setenv_path = AppConfig.get_setevn_path()
    a = locals()
    b = globals()
    a.update(b)

    initdb_cmd_str = Template(
        "mkdir ${db_data_home};. ${setenv_path}; initdb -E UTF8 -D ${db_data_home}/data").substitute(a)

    print ("initdb ", initdb_cmd_str)
    subprocess.call(initdb_cmd_str, shell=True)
    if allow_connections_from_subnet:
        subprocess.call(Template(
            """echo "listen_addresses= \'*\'" >> ${db_data_home}/data/postgresql.conf""").substitute(a), shell=True)
        subprocess.call(Template(
            """echo "host all all 0.0.0.0/0 trust" >> ${db_data_home}/data/pg_hba.conf""").substitute(a), shell=True)

    startdb(db_data_home, port, db_cfg_param_val_dict=db_cfg_param_val_dict,
            log_statement=log_statement)


def pingdb(dbdesc=None, iambash=False):
    port = pgu.get_db_port(dbdesc)
    p = subprocess.Popen(Template('''psql -p $port postgres -t -c "select 5+6"''').substitute(
        locals()), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    ping_res = p.stdout.readline().rstrip()
    dbstatus = 0  # 0 is dead; 1 is alive
    dbstatus_msgs = ["failed: no alive db on port " +
                     str(port), "success: db on port " + str(port)]
    try:
        ping_res_as_int = int(ping_res)
        if ping_res_as_int == 11:
            dbstatus = 1
    except:
        dbstatus = 0  # need a noop here or better programming practice
    # how the result needs to be outputted
    if iambash:
        print(dbstatus)
    else:
        return dbstatus_msgs[dbstatus]


def startdb(db_data_home, port=None, db_cfg_param_val_dict=None, log_statement='none'):
    assert(port is not None)
    assert(db_data_home is not None)
    default_db_cfg_param_val_dict = dict()
    default_db_cfg_param_val_dict['shared_buffers'] = "50MB"
    default_db_cfg_param_val_dict['maintenance_work_mem'] = "50MB"
    default_db_cfg_param_val_dict['work_mem'] = "50MB"
    default_db_cfg_param_val_dict['effective_cache_size'] = "50MB"
    default_db_cfg_param_val_dict['max_wal_size'] = "50MB"
    default_db_cfg_param_val_dict['checkpoint_timeout'] = "5min"
    default_db_cfg_param_val_dict['log_statement'] = "all"
    default_db_cfg_param_val_dict['max_connections'] = "5"

    if db_cfg_param_val_dict is not None:
        default_db_cfg_param_val_dict.update(db_cfg_param_val_dict)

    print ("cfg param = ", default_db_cfg_param_val_dict)
    setenv_path = AppConfig.get_setevn_path()
    start_cmd_template = Template(". ${setenv_path}; pg_ctl -o \" -F -p ${port} -c log_connections=True -c log_disconnections=True -c fsync=off -c shared_buffers=${shared_buffers} -c maintenance_work_mem=${maintenance_work_mem} -c work_mem=${work_mem} -c effective_cache_size=${effective_cache_size}  -c bgwriter_delay=10000ms -c wal_writer_delay=10000ms -c max_wal_size=${max_wal_size} -c checkpoint_timeout=${checkpoint_timeout} -c full_page_writes=off -c synchronous_commit=off -c log_statement=${log_statement} -c max_connections=${max_connections}\" -D ${db_data_home}/data -l ${db_data_home}/logfile -w start")

    a = locals()
    b = globals()
    a.update(b)
    a.update(default_db_cfg_param_val_dict)

    start_cmd_str = start_cmd_template.substitute(a)
    print (start_cmd_str)
    subprocess.call(start_cmd_str, shell=True)
    pgu.create_extension_postgis(port)


def stopdb(db_data_home):
    setenv_path = AppConfig.get_setevn_path()
    stopdb_cmd_str = f'. {setenv_path}; pg_ctl -D {db_data_home}/data stop'
    subprocess.call(stopdb_cmd_str, shell=True)
    return 1 #TODO: assuming this to be always success; TBF


def savedb(user, db_data_home, image_identifier, target_node='127.0.0.1', target_dir='/media/kabira/home/DBbackups/'):
    db_data_tar_fn = os.path.dirname(db_data_home)+"/data.tar"
    db_data_tar_remote_fn = Template(
        "${target_dir}/${image_identifier}.tar").substitute(locals())
    # use parallel tar and paralle zip command
    tar_cmd_template = Template(
        "cd ${db_data_home};  tar cf ${db_data_tar_fn} data")
    a = locals()
    b = globals()
    a.update(b)
    tar_cmd_str = tar_cmd_template.substitute(a)
    print(tar_cmd_str)
    subprocess.call(tar_cmd_str, shell=True)
    # save_cmd_str=Template("rsync ").substitute(locals()) #TODO: for now do simple rsync
    #subprocess.call(save_cmd_str, shell=True)
