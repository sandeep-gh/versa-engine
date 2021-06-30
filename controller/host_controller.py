
'''
start/stop/resume a database on the host machine
'''
from string import Template
import getpass

from controller import pgsa_utils as pgu, host_controller_impl as impl
from common import utilities

dbuser=getpass.getuser()
def initdb(dbdesc=None, host_type='standard', log_statement='none', db_cfg_param_val_dict=dict()):
    '''
    initialize and start database
    '''
    port = pgu.get_db_port(dbdesc)
    db_data_home=pgu.get_db_data_home(dbdesc,dbuser, port)
    param_val_dict = {} # TODO: needs to be optional utilities.get_db_system_param_value_dict(host_type)
    param_val_dict.update(db_cfg_param_val_dict)
    impl.initdb(db_data_home, port, db_cfg_param_val_dict = param_val_dict, log_statement=log_statement)


def startdb(dbdesc=None, host_type='standard', log_statement='none', db_cfg_param_val_dict=dict()):
    global dbuser
    port = pgu.get_db_port(dbdesc)
    db_data_home=pgu.get_db_data_home(dbdesc,dbuser, port)

    param_value_dict = {} #utilities.get_db_system_param_value_dict(host_type) TODO: this needs fixing
    param_value_dict.update(db_cfg_param_val_dict)
    impl.startdb(db_data_home, port, db_cfg_param_val_dict=param_value_dict, log_statement=log_statement)


def cleanup(dbdesc=None):
    #pgu.close_indemics_server(user=dbuser)
    port = pgu.get_db_port(dbdesc)
    db_data_home=pgu.get_db_data_home(dbdesc=dbdesc, user=dbuser, port=port)
    impl.stopdb(db_data_home)
    utilities.remove_dir(db_data_home)

def savedb(dbdesc=None, port=None):
    '''
    stop the database; copy the image to remote location; and restart the database
    '''
    port = pgu.get_db_port(dbdesc)
    db_data_home=pgu.get_db_data_home(dbdesc=dbdesc, user=dbuser, port=port)
    impl.stopdb(db_data_home)
    impl.savedb(dbuser, db_data_home, dbdesc)
    impl.startdb(dbdesc)


def restoredb(dbdesc=None):
    '''
    fetch remote dbimage and start the database
    '''
    port = pgu.get_db_port(dbdesc)
    pgu.rcp_db_img(user=dbuser, port=port, dbdesc=dbdesc)
    db_data_home=pgu.get_db_data_home(dbdesc=dbdesc, user=dbuser, port=port)
    impl.startdb(db_data_home, port=port)


def stopdb(dbdesc=None):
    port = pgu.get_db_port(dbdesc)
    db_data_home=pgu.get_db_data_home(dbdesc,dbuser, port)
    impl.stopdb(db_data_home)

def pingdb(dbdesc=None, iambash=False):
    session_fn = pgu.get_session_fn(dbdesc)
    session_module = utilities.import_module(module_name = session_fn) #TODO: this check should be part of decorator
    if session_module is None:
        return "Invalid dbsession" #TODO: better to throw exception
    return impl.pingdb(dbdesc, iambash)

def closedb(dbdesc=None):
    '''
    Raises the closedb flag; so that the launcher can close
    the database on next polling. currently not functional
    '''
    set_dbdesc(dbdesc)
    #work_dir=pgu.get_work_dir(dbdesc)
    a = locals()
    b = globals()
    a.update(b)
    closedb_flag_fn=closedb_flag_fn_template.substitute(a)
    cmd_str="touch "+closedb_flag_fn
    subprocess.call(cmd_str, shell=True)
