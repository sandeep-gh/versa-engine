#!/usr/bin/env python3
# from jobmanagers import get_jobmanager
# import rmo.versa_impl as vi
# import rmo.utils as vu
import argparse
import os
import sys
from datetime import datetime

import versa_engine as ve
mu = ve.metadata_utils
edi = ve.edi.dicex_edi_impl
vu = ve.rmo.utils
vi = ve.rmo.versa_impl

# from versa_engine import metadata_utils as mu, dicex_edi_impl as dei, rmo.utils as vu, rmo.versa_impl as vi
# import common.metadata_utils as mu
# import common.utilities as utilities
# import controller.frontend_controller as cntrl
# import edi.dicex_edi_impl as dei


parser = argparse.ArgumentParser()
parser.add_argument("--port", help="specify port number",
                    nargs='?', const=None)
parser.add_argument(
    "--work_dir", help="where are all output/config/temps are written down", nargs='?')
parser.add_argument("--force_create_model",
                    help="build model and load data even if model exists", action='store_true')
parser.add_argument("--session_id", nargs=1, help="dbsession identifier")
parser.add_argument(
    "--launch", help="launch database session", action='store_true')
parser.add_argument("--host_type", help="default is dedicated; largemem for 512GB smp machines",
                    nargs='?', choices=['standard', 'largemem'])
parser.add_argument(
    "--remove", help="remove/cleanup database session", action='store_true')
parser.add_argument("--walltime_hours",
                    help="number of hours for running a database session", nargs='?')
parser.add_argument("--log_statement", help="none, ddl, mod, all")


parser.add_argument("--build_rmo_file", nargs=2,
                    help="takes two arguments; the location of the metadata and the data. ")
#parser.add_argument("--build_orm", nargs=2, help="build an orm model")
parser.add_argument("--data_config", nargs=1,
                    help="external data configuration file")
#parser.add_argument("--load_", action='store_true', help="build orms for all tables and files in edcfg")
#parser.add_argument("--build_pg_rmo", action='store_true', help="build orms for postgres tables")
parser.add_argument("--only_rmo", action='store_true',
                    help="don't ingest the data; only build the rmo wrapper")
parser.add_argument("--build_rmo", nargs=1,
                    help="don't ingest the data; only build the rmo wrapper")

#parser.add_argument("--build_metadata", action='store_true', help="generate the metadata for remote tables (currently only works for postgres tables)")
parser.add_argument("--make_local_copy", action='store_true',
                    help="make a local copy of the remote table; False by default; is overwrriten by per table config description")


args = parser.parse_args()
force_create_model = False
# build_metadata=False
make_local_copy = False

session_name = None
log_statement = 'none'
walltime_hours = "1"

session_run_dir = './'
if 'espresso_session_run_dir' in os.environ:
    session_run_dir = os.environ['espresso_session_run_dir']

# if not args.launch and not args.remove:

# if not args.launch and not args.remove:  #use launch as default if not specified
#    args.launch = True


if args.session_id:
    session_name = args.session_id[0]
else:
    if args.launch:  # no session id given
        session_name = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    else:
        session_name = vu.get_latest_dbsession(session_run_dir)
        if session_name is None:
            print("no session found to be removed..exiting")
            sys.exit()

assert session_name is not None


if args.remove:
    #TODO: removejob
    # need removejob is jobmanager; for now use localhostjobmanagers
    # dpi.removejob(session_name)
    
    sys.exit()

if args.walltime_hours:
    walltime_hours = args.walltime_hours

if args.log_statement:
    log_statement = args.log_statement


if args.launch:
    try:
        # first launch... dbsession file does not exists
        if not os.path.isfile(session_fn):
            pass
        else:
            session_fn = pgu.get_session_fn(dbdesc)
            # todo: handle error if there is  no session
            vi.init(session_name, session_run_dir)
            print(
                "session already active and running; try different session name;..exiting")
            dei.close_session()
            sys.exit()
    except Exception as e:
        pass

    # using default launch values
    # TODO: nested arguments are hard
    max_wal_size = '4GB'
    checkpoint_timeout = '30min'
    shared_buffer_size = '4GB'
    run_mode = 'default'
    if args.host_type == 'largemem':
        shared_buffer_size = '128GB'
        max_wal_size = '32GB'
        checkpoint_timeout = '60min'
        cntrl.launchdbjob(session_name, postjob_cleanup=True, host_type='largemem',
                          log_statement=log_statement, run_mode=run_mode, walltime_hours=walltime_hours, )
    else:
        cntrl.launchdbjob(session_name,  postjob_cleanup=True, log_statement=log_statement,
                          run_mode=run_mode, walltime_hours=walltime_hours, jobmanager=get_jobmanager())


try:
    # todo: handle error if there is  no session
    vi.init(session_name, session_run_dir)
except Exception as e:
    print("data session is not active;exiting.." + str(e))
    sys.exit()


if args.force_create_model:
    force_create_model = True

if args.make_local_copy:
    make_local_copy = True

if args.build_rmo:  # build rmo wrapper for the metadata
    metadata_root = mu.read_metadata(args.build_rmo[0])
    # TODO: what about model_name
    vi.build_only_orm_from_metadata(metadata_root)

if args.build_rmo_file:
    vi.wrap_file(session_name, args.wrap_file[0], args.wrap_file[1])

work_dir = "./"
if args.work_dir:
    work_dir = args.work_dir

edcfg_xml = None
if args.data_config:
    edcfg_xml = args.data_config[0]
    # the recursive loader
    vi.build_orms(edcfg_xml, work_dir,  force_create_model=force_create_model)

    # currently automatically copying local data from remote source is turned off
    vi.build_orm_oracle_tables(
        edcfg_xml, make_local_copy=make_local_copy, force_create_model=force_create_model)  # fix work dir here

    # TODO: we need to fix this
    vi.build_orm_pg_tables(
        edcfg_xml, force_create_model=force_create_model, make_local_copy=make_local_copy)  # fix work dir here

    # vi.build_orm_gp_pipeline(edcfg_xml)
# TODO: how do we deal with make_local_copy and  only_rmo

# if args.build_only_orms:
#       #TODO: we need to fix this
#     #vi.build_orm_ontables(edcfg_xml, force_create_model=force_create_model, make_local_copy=make_local_copy)
#     vi.build_only_orms(edcfg_xml, force_create_model=force_create_model)

# if args.build_pg_orms:
#     vi.build_orm_pg_tables(edcfg_xml, force_create_model=force_create_model, make_local_copy=make_local_copy)


dei.close_session()  # close connection to the database
