#from utilities import system_config_root
from string import Template
import subprocess
import os
from multiprocessing import Process
import versa_engine.controller.pgsa_utils as pgu
# '''
# Interface to job execution via qsub
# '''

jobmanager_dir = os.path.dirname(os.path.realpath(__file__))


def get_job_submission_args(host_type='standard'):
    return None


def get_pgsa_launch_template():
    pgsa_launch_qsub_template = Template(open(
        jobmanager_dir+"/pgsa.sh.template").read())  # TODO: put pgsa.qsub.template here
    # do safe substitue here and then send
    #[qsub_group_list, qsub_q, qsub_ppn]  = get_job_submission_args(host_type)
    return pgsa_launch_qsub_template


def launch_job(launch_script_fn):
    try:
        #subprocess.call("sh " + launch_script_fn, shell=True)
        subprocess.Popen(['sh', launch_script_fn])
    except subprocess.CalledProcessError as e:
        print("Unable to launch:\n", e.output)


def get_pgsa_cleanup_template():
    pgsa_cleanup_sh_template = Template(
        open(jobmanager_dir+"/pgsa_cleanup.sh.template").read())
    return pgsa_cleanup_sh_template


def launch_cleanup_job(pid, pgsa_cleanup_script_fn):
    post_cleanup_job_cmd = Template(
        "tail --pid=$pid -f /dev/null; sh  ${pgsa_cleanup_script_fn}").substitute(locals())
    print("cleanup launch job = ", post_cleanup_job_cmd)
    try:
        subprocess.call(post_cleanup_job_cmd, shell=True)
    except subprocess.CalledProcessError as e:
        print("Unable to launch:\n", e.output)


def removejob(dbdesc):
    set_dbdesc(dbdesc)
    exec_remote_cmd(dbdesc, "cleandb")
    jobid = pgu.get_jobid(dbdesc)
    if jobid is not None:
        subprocess.call("qdel "+jobid, shell=True)
    else:
        print("Error in remove job")


def get_jobid(dbdesc):
    return pgu.get_pgpid(dbdesc)


def transfer_file(source_host, source_dir, source_fn, target_host, target_dir, target_fn):
    assert source_host == target_host
    subprocess.call(['cp', source_dir + "/" + source_fn,
                     target_dir + "/" + target_fn], shell=True)


def get_launch_frontend_proxy_template():
    '''
    launch frontend proxy on host machine
    '''
    template = Template(open(
        jobmanager_dir+"/frontendproxy.sh.template").read())  # TODO: put pgsa.qsub.template here
    # do safe substitue here and then send
    #[qsub_group_list, qsub_q, qsub_ppn]  = get_job_submission_args(host_type)
    return template


def launchdbjob(dbdesc=None, dbport=None, save_on_exit_val="no", postjob_cleanup=False, host_type='standard', log_statement='all', run_mode='default', walltime_hours=1, run_dir="./", jobmanager=None):
    '''
    postjob_cleanup : add anthor job dependent on this to perform cleanup
    '''
    if (dbdesc == None) or (dbdesc == "undef"):
        timestamp = pgu.get_timestamp()
        dbdesc = desc_template.substitute(locals())
    # set_dbdesc(dbdesc)
    work_dir = tempfile.mkdtemp()
    work_dir = "/tmp/tmp5gbgkb_2/"
    run_dir = work_dir

    try:
        if not os.path.exists(run_dir):
            os.mkdir(run_dir)
    except Exception as e:
        print("got exception ", str(e))
    closedb_flag_fn = work_dir.strip() + "/" + dbdesc
    port_server_ip = AppConfig.get_port_server_ip()
    ######################################################
    # server_port: 1.Use specified port,
    #              2. Use the one specified in dbsession
    #              3. create new one
    #
    ########################################################
    if dbport is None:
        dbport = utilities.get_new_port(port_server_ip=port_server_ip)

    db_data_home = pgu.get_db_data_home(
        dbdesc=dbdesc, user=dbuser, port=dbport, wdir=work_dir)

    # TODO: use path based on pgversion

    cluster_name = AppConfig.get_cluster_name()
    pybin_path = AppConfig.get_pybin_path()
    start_cmd = Template(
        "--dbsession ${dbdesc} --run_mode ${run_mode} --host_type ${host_type} --log_statement ${log_statement} --run_dir ${run_dir}  --startdb ").substitute(locals())
    ping_cmd = Template(
        "--dbsession ${dbdesc} --pingdb --iambash").substitute(locals())
    message_port = int(utilities.get_new_port(port_server_ip))
    hostname = utilities.gethostname()
    assert 'dicex_basedir' in os.environ
    dicex_basedir = os.environ['dicex_basedir']
    # TODO: setenv_path  variable is hardwired --> they need to come from somewhere
    setenv_path = '~/.versa/env.sh'  # TODO: this should come from config
    postdb_start_script = ""
    postdb_stop_script = ""

    if run_dir is None:
        run_dir = os.getcwd()
    a = locals()
    b = globals()
    a.update(b)
    pgsa_joblaunch_template = get_pgsa_launch_template()
    pgsa_joblaunch_str = pgsa_joblaunch_template.safe_substitute(a)
    # write pgsa_joblaunch_str to file pgsa.job in dir(work_dir) and invoke jobmanager.launch_job
    pgsa_job_fn = work_dir+"/pgsa.job"

    pgsa_job_fh = open(pgsa_job_fn, 'w+')
    pgsa_job_fh.write(pgsa_joblaunch_str)
    pgsa_job_fh.close()
    # we shall redirect output to devnll

    launch_job(pgsa_job_fn)  # launch job asynchronously/background
    print("waiting for database to start...")
    message_port_listener = utilities.get_listener(message_port)
    db_launch_msg = utilities.signal_listen(message_port_listener)

    if db_launch_msg[0] == "database started":
        print("database session successfully  started on host " +
              pgu.get_db_host(dbdesc, wdir=run_dir))

        def launch_frontend_proxy(jobmanager, run_dir=run_dir, dicex_basedir=dicex_basedir, frontend_proxy_port=None):
            pybin_path = AppConfig.get_pybin_path()
            joblaunch_str = get_launch_frontend_proxy_template().safe_substitute(locals())
            job_fn = work_dir+"/frontendproxy.job"
            job_fh = open(job_fn, 'w+')
            job_fh.write(joblaunch_str)
            job_fh.close()
            launch_job(job_fn)  # launch job asynchronously/background

            while 1:
                time.sleep(2)
                try:
                    # could not be a localhost to
                    conn = rpyc.connect("localhost", frontend_proxy_port)
                except Exception as e:
                    pass
                if conn is not None:
                    break

            return conn

        frontend_proxy_port = utilities.get_new_port(
            port_server_ip=port_server_ip)
        conn = launch_frontend_proxy(
            jobmanager, frontend_proxy_port=frontend_proxy_port)

        return ProxyConnectionCtx(frontend_proxy_port, dbport, conn)
    else:  # db_launch_msg == "database launch failed":
        print("Failed: can not start a database session....")
        #print (db_launch_msg)
        #print ("report issue at https://ndsslgit.vbi.vt.edu/ndssl-software/dicex_epistudy/issues")
        return None  # exit status
