#!/usr/bin/env python3

import socket
import argparse

import versa_engine as ve
#from ..controller.frontend_controller import launchdbjob, removedbjob

#from jobmanagers import get_jobmanager

parser = argparse.ArgumentParser()
parser.add_argument("--dbsession", help="the dbsession description", nargs=1)
parser.add_argument(
    "--launchjob", help="launch a database job, option dbdesc", action='store_true')
parser.add_argument(
    "--removejob", help="remove a database job, option dbdesc", action='store_true')


# other options
parser.add_argument("--postjob_cleanup", help="lauch cleanup post completion of dbsession",
                    nargs='?', const="yes", default="yes")
parser.add_argument("--postdb_start", nargs=1,
                    help="path of script to be run after db start")
parser.add_argument("--postdb_stop", nargs=1,
                    help="path of script to be run after db stop")
parser.add_argument(
    "--walltime", help="number of hours for running a database session", nargs='?')
parser.add_argument("--save_on_exit", help="save dbsession on database exit",
                    nargs='?', const="no", default="no")
parser.add_argument(
    "--host_type", help="default is dedicated; largemem for 512GB smp machines", nargs='?')
parser.add_argument("--run_mode", help="optimized or default", nargs='?')
parser.add_argument("--log_statement", help="none, ddl, mod, all")


args = parser.parse_args()
dbsession_dbdesc = args.dbsession[0]
# max_wal_size='8GB'
# checkpoint_timeout='30min'
# shared_buffer_size='8GB'
log_statement='none'
run_mode='default'

#TODO
# if args.postdb_start:
#     dpi.postdb_start_script=args.postdb_start[0]

# if args.postdb_stop:
#     dpi.postdb_stop_script=args.postdb_stop[0]

#if args.walltime:
#    dpi.dbsession_walltime=args.walltime

if args.log_statement:
    log_statement = args.log_statement

if args.run_mode:
    run_mode = args.run_mode
###################################
## execution commands
###################################
if args.launchjob:
    ve.launchdbjob(dbdesc=dbsession_dbdesc,  postjob_cleanup= args.postjob_cleanup =='yes', run_mode=run_mode,jobmanager=ve.get_jobmanager())

#if args.removejob:
#    removedbjob(dbsession_dbdesc)
