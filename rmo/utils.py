import sys
import os
import imp
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tabulate import tabulate
from pandas import DataFrame
from string import Template

import common.xmlutils as xu
import common.utilities as utilities
import controller.pgsa_utils as pgu
import rmo.versa_api_meta as vam


def import_model(model_name, model_dir=None):
    try:
        if model_dir is None:
            model_dir = os.getcwd()
        sys.path.append(model_dir) 
        model_fn=model_name + "_model"
        imp.find_module(model_fn)
        model_obj=__import__(model_fn)
    except:
        found=False
        print("import error:" + model_name)
        assert(0)
    return getattr(model_obj, model_name)

def check_module_exists(module_name):
    try:
        sys.path.append(os.getcwd())
        imp.find_module(module_name)
        model_obj=__import__(module_name)
        return True
    except ImportError:
        found=False
        return False
        

def check_model_exists(model_name=None):
    return check_module_exists(model_name + "_model")


def rename_col(model_name, col_name):
    fkeys=getattr(model_name, '_foreignkeys')
    if col_name in fkeys:
        col_name = 'f_'+col_name
    return col_name


def build_primary_key(primary_keys, foreign_keys):
    key_str=''
    all_keys = primary_keys
    for k in all_keys:
        key = k.lower()
        if key in foreign_keys:
            key= 'f_' + key
        key_str = key_str + "\\'" + key.strip().lower() + "\\',"
    return key_str[:-1]

def build_clone_primary_key(primary_keys, foreign_keys):
    key_str=''
    all_keys = primary_keys
    for k in all_keys:
        key = k.lower()
        if key in foreign_keys:
            key= 'f_' + key
        key_str = key_str + "\\'" + 'm_' + key.strip().lower() + "\\',"
    return key_str[:-1]

def get_latest_dbsession(work_dir="./"):
    """
    get least recently used session id. Returns none if none exists. 

    work_dir: None specifies current directory; currently not implemented
    """
    lru_dbsession  = utilities.get_last_file_by_pattern("dbsession*.py", work_dir) #lru: least recently used
    if lru_dbsession is None:
        print("no dbsession found in the current directory")
        return None

    session_name = lru_dbsession[10:-3] #extract the session name
    return session_name
    

def build_session(dbsession=None, port=None, conn_remote=False, session_run_dir='./'):
    '''
    conn_remote: if the connection is from a remote machine. We make this distinction 
    because we think local connections are less likely to break'''

    if dbsession is None:
        dbsession = get_latest_dbsession(session_run_dir)


    assert dbsession is not None

    if port is None:
        port = pgu.get_db_port(dbsession, wdir=session_run_dir)
    dbhost = 'localhost'
    if conn_remote: 
        dbhost=pgu.get_db_host(dbsession, wdir=session_run_dir)
    username = utilities.get_username()
    conn_str='postgresql+psycopg2://'+username+'@' + dbhost + ':' + str(port) + '/postgres' #TODO: connection string should be build through a function call
    engine=create_engine(conn_str)
    Session = sessionmaker()
    Session.configure(bind=engine)
    session=Session()
    return session

