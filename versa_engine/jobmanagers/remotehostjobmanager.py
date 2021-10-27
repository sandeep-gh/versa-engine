# from utilities import system_config_root
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
    # [qsub_group_list, qsub_q, qsub_ppn]  = get_job_submission_args(host_type)
    return pgsa_launch_qsub_template


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


def launchdbjob(remotehost, session_id, local_run_dir, **kwargs):
    ip, port = AppConfig.get_remotehost_conninfo(hostname)
    session_id
    return launchdbjob_impl(ip, port, session_did, **kwargs)
    
    pass

def launchdbjob_impl(sessionLauncherService_ip, sessionLauncherService_port,  session_id, local_run_dir, remote_run_dir, **kwargs):
    """
    launch job on the remote server
    """
    sls_conn = rpyc.connect(sessionLauncherService_ip,
                            sessionLauncherService_port)
    dbsession_ctx = sls_conn.root.launchdbjob(kwargs)
    return dbsession_ctx
    # write out dbsession_ctx to local run dir

    pass
