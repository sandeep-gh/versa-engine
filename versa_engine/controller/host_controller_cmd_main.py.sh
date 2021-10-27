#!/usr/bin/env python

#TODOs: multiple file arguments
import argparse
from versa_engine.jobmanagers.jobmanager import get_jobmanager
import versa_engine.controller.host_controller as cntrl

parser = argparse.ArgumentParser()


parser.add_argument("--dbsession", help="the dbsession description", nargs=1)
parser.add_argument("--run_dir", help="the dbsession description", nargs=1)
parser.add_argument("--host_type", help="default is dedicated; largemem for 512GB smp machines", nargs='?')
###########################################################
##DB image save and resume
###########################################################
parser.add_argument("--savedb", help="save the database session", action='store_true')
parser.add_argument("--restoredb",help="restore the database session", action='store_true')
parser.add_argument("--shared_buffer_size",help="size of the shared buffer, e.g., 8GB")
parser.add_argument("--max_wal_size",help="size of dirty pages, e.g., 8GB")
parser.add_argument("--checkpoint_timeout",help="checkpoint timeout e.g., 30min")
parser.add_argument("--maintenance_work_mem",help="maintenance_work_mem, default 10GB")
parser.add_argument("--work_mem",help="work_mem, default 2GB")
parser.add_argument("--effective_cache_size",help="effective_cache_size 10GB")
parser.add_argument("--max_connections",help="max_connections, default 5")
parser.add_argument("--jobmanager", help="what kind of setup is the app running on ",  default="localhost")
parser.add_argument("--run_mode",help="run mode: optimized or default")
parser.add_argument("--log_statement",help="none, ddl, mod, all")



###########################################################
##manage supporting script
###########################################################
parser.add_argument("--restartdb", action='store_true', help="restart a stopped database")
parser.add_argument("--iambash", action='store_true', help="this call is from bash script")
parser.add_argument("--stopdb", action='store_true', help="stop database")
#parser.add_argument("--closedb", help="raise flag to close the database session", action='store_true' )
parser.add_argument("--startdb", help="init database", action='store_true')
parser.add_argument("--cleandb",help='stop and delete data files', action='store_true')
parser.add_argument("--pingdb",help='check if database is running', action='store_true')




args = parser.parse_args()
dbsession_dbdesc = args.dbsession[0]
host_type='standard'
log_statement='none'
jobmanager = get_jobmanager() #TODO:user input from frontend or from config needs to reach here
if args.host_type:
    host_type = args.host_type
db_cfg_param_val_dict = dict()
if args.shared_buffer_size:
    db_cfg_param_val_dict['shared_buffers'] = args.shared_buffer_size
    
if args.max_wal_size:
    db_cfg_param_val_dict['max_wal_size'] = args.max_wal_size

if args.maintenance_work_mem:
    db_cfg_param_val_dict['maintenance_work_mem'] = args.maintenance_work_mem

if args.work_mem:
    db_cfg_param_val_dict['work_mem'] = args.work_mem

if args.effective_cache_size:
    db_cfg_param_val_dict['effective_cache_size'] = args.effective_cache_size

if args.checkpoint_timeout:
    db_cfg_param_val_dict['checkpoint_timeout'] = args.checkpoint_timeout

if args.max_connections:
    db_cfg_param_val_dict['max_connections'] = args.max_connections

#if args.run_mode:
   # if args.run_mode == 'optimized':
        #pgu.log_statement = 'none' #TODO: fix this
 
if args.log_statement:
    log_statement = args.log_statement

run_dir = "./"
if args.run_dir:
    run_dir = args.run_dir[0]
if args.startdb:
    cntrl.initdb(dbdesc=dbsession_dbdesc, host_type=host_type, log_statement=log_statement, db_cfg_param_val_dict = db_cfg_param_val_dict, wdir=run_dir)

if args.restartdb:
    cntrl.startdb(dbsession_dbdesc, host_type=host_type, log_statement=log_statement, db_cfg_param_val_dict= db_cfg_param_val_dict, wdir=run_dir)


if args.cleandb: ## stop and remove files
    cntrl.cleanup(dbdesc=dbsession_dbdesc, wdir=run_dir)
    

if args.savedb:
    cntrl.savedb(dbdesc=dbsession_dbdesc, wdir=run_dir)

if args.restoredb:
    cntrl.resumedb(dbdesc=dbsession_dbdesc, wdir=run_dir)
    
if args.stopdb:
    cntrl.stopdb(dbdesc=dbsession_dbdesc, wdir=run_dir)


#if args.closedb:
#    cntrl.closedb(dbsession_dbdesc)

if args.pingdb:
    cntrl.pingdb(dbsession_dbdesc, args.iambash, run_dir)
    
#################################
##TODO: path and python enviorment
##################################
