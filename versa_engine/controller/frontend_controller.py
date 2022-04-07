'''
Create a launcher script from command line arguments
'''
import tempfile
import getpass
from string import Template
import os
from typing import NamedTuple, Any
import rpyc
import time
import subprocess

from versa_engine.common import utilities
from versa_engine.common import xmlutils as xu
from versa_engine.controller import pgsa_utils as pgu
from versa_engine.controller.appconfig import AppConfig
from versa_engine.jobmanagers.jobmanager import get_jobmanager

dbuser = getpass.getuser()  # the db user is same as logined user


class ProxyConnectionCtx(NamedTuple):
    proxy_port: Any
    dbport: Any
    conn: Any
    pass


def launchdbjob(**kwargs):
    jobmanager_type = kwargs.pop("jobmanager_type", "localhost")
    jobmanager = get_jobmanager(jobmanager_type)

    return jobmanager.launchdbjob(**kwargs)

# def launchdbjob(dbdesc=None, dbport=None, save_on_exit_val="no", postjob_cleanup=False, host_type='standard', log_statement='all', run_mode='default', walltime_hours=1, run_dir="./", jobmanager=None):
#     '''
#     postjob_cleanup : add anthor job dependent on this to perform cleanup
#     '''
#     if (dbdesc == None) or (dbdesc == "undef"):
#         timestamp = pgu.get_timestamp()
#         dbdesc = desc_template.substitute(locals())
#     # set_dbdesc(dbdesc)
#     work_dir = tempfile.mkdtemp()
#     work_dir = "/tmp/tmp5gbgkb_2/"
#     run_dir = work_dir

#     try:
#         if not os.path.exists(run_dir):
#             os.mkdir(run_dir)
#     except Exception as e:
#         print ("got exception ", str(e))
#     closedb_flag_fn = work_dir.strip() + "/" + dbdesc
#     port_server_ip = AppConfig.get_port_server_ip()
#     ######################################################
#     # server_port: 1.Use specified port,
#     #              2. Use the one specified in dbsession
#     #              3. create new one
#     #
#     ########################################################
#     if dbport is None:
#         dbport = utilities.get_new_port(port_server_ip=port_server_ip)

#     db_data_home = pgu.get_db_data_home(
#         dbdesc=dbdesc, user=dbuser, port=dbport, wdir=work_dir)

#     # TODO: use path based on pgversion

#     cluster_name = AppConfig.get_cluster_name()
#     pybin_path = AppConfig.get_pybin_path()
#     start_cmd = Template(
#         "--dbsession ${dbdesc} --run_mode ${run_mode} --host_type ${host_type} --log_statement ${log_statement} --run_dir ${run_dir}  --startdb ").substitute(locals())
#     ping_cmd = Template(
#         "--dbsession ${dbdesc} --pingdb --iambash").substitute(locals())
#     message_port = int(utilities.get_new_port(port_server_ip))
#     hostname = utilities.gethostname()
#     assert 'dicex_basedir' in os.environ
#     dicex_basedir = os.environ['dicex_basedir']
#     # TODO: setenv_path  variable is hardwired --> they need to come from somewhere
#     setenv_path = '~/.versa/env.sh'  # TODO: this should come from config
#     postdb_start_script = ""
#     postdb_stop_script = ""


#     if run_dir is None:
#         run_dir = os.getcwd()
#     a = locals()
#     b = globals()
#     a.update(b)
#     if jobmanager is None:
#         jobmanager = get_jobmanager()
#     pgsa_joblaunch_template = jobmanager.get_pgsa_launch_template()
#     pgsa_joblaunch_str = pgsa_joblaunch_template.safe_substitute(a)
#     # write pgsa_joblaunch_str to file pgsa.job in dir(work_dir) and invoke jobmanager.launch_job
#     pgsa_job_fn = work_dir+"/pgsa.job"

#     pgsa_job_fh = open(pgsa_job_fn, 'w+')
#     pgsa_job_fh.write(pgsa_joblaunch_str)
#     pgsa_job_fh.close()
#     # we shall redirect output to devnll

#     jobmanager.launch_job(pgsa_job_fn)  # launch job asynchronously/background
#     print("waiting for database to start...")
#     message_port_listener = utilities.get_listener(message_port)
#     db_launch_msg = utilities.signal_listen(message_port_listener)


#     # launch job for cleaning
#     # if postjob_cleanup:
#     #     #what is the jobid
#     #     jobid = jobmanager.get_jobid(dbdesc)
#     #     remove_cmd=Template("--dbsession ${dbdesc} --removejob").substitute(locals())
#     #     a = locals()
#     #     b = globals()
#     #     a.update(b)
#     #     pgsa_jobcleanup_str = jobmanager.get_pgsa_cleanup_template().safe_substitute(a) # pgsa_cleanup_qsub_template.safe_substitute(a)
#     #     print ("postjob cleanup = ", pgsa_jobcleanup_str)
#     #     pgsa_jobcleanup_fn = work_dir+"/pgsa_cleanup.job"
#     #     pgsa_jobcleanup_fh = open(pgsa_jobcleanup_fn, 'w+')
#     #     pgsa_jobcleanup_fh.write(pgsa_jobcleanup_str)
#     #     pgsa_jobcleanup_fh.close()
#     #     jobmanager.launch_cleanup_job(jobid, pgsa_jobcleanup_fn)
#     if db_launch_msg[0] == "database started":
#         print("database session successfully  started on host " +
#               pgu.get_db_host(dbdesc, wdir=run_dir))
#         def launch_frontend_proxy(jobmanager, run_dir=run_dir, dicex_basedir=dicex_basedir, frontend_proxy_port=None):
#             pybin_path = AppConfig.get_pybin_path()
#             joblaunch_str = jobmanager.get_launch_frontend_proxy_template().safe_substitute(locals())
#             job_fn = work_dir+"/frontendproxy.job"
#             job_fh = open(job_fn, 'w+')
#             job_fh.write(joblaunch_str)
#             job_fh.close()
#             jobmanager.launch_job(job_fn)  # launch job asynchronously/background

#             while 1:
#                 time.sleep(2)
#                 try:
#                     conn = rpyc.connect("localhost", frontend_proxy_port) #could not be a localhost to
#                 except Exception as e:
#                     pass
#                 if conn is not None:
#                     break

#             return conn

#         frontend_proxy_port = utilities.get_new_port(port_server_ip=port_server_ip)
#         conn = launch_frontend_proxy(jobmanager, frontend_proxy_port=frontend_proxy_port)

#         return ProxyConnectionCtx(frontend_proxy_port, dbport, conn)
#     else:  # db_launch_msg == "database launch failed":
#         print("Failed: can not start a database session....")
#         # print (db_launch_msg)
#         # print ("report issue at https://ndsslgit.vbi.vt.edu/ndssl-software/dicex_epistudy/issues")
#         return None  # exit status


def exec_remote_cmd(dbdesc, remote_cmd):
    work_dir = pgu.get_work_dir(dbdesc)
    db_hostname = pgu.get_db_host(dbdesc)
    dicex_basedir = utilities.get_dicex_basedir()
    cluster_name = pgu.get_cluster_name(dbdesc)
    a = locals()
    b = globals()
    a.update(b)

    cmd_str = Template("ssh ${db_hostname} \"" + ". " + dicex_basedir + "/dicex_${cluster_name}.sh; " +
                       " cd ${work_dir};${dicex_pgsa_dir}/dicex_pgsa_dbhost.py.sh --dbsession ${dbdesc} --${remote_cmd}\"").substitute(a)
    subprocess.call(cmd_str, shell=True)


def removedbjob(dbdesc):
    print("not implemented yet")
    # call the job manager to remove process/job etc)
