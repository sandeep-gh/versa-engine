from common.utilities import system_config_root #TODO: not sure if this is the right approach

import controller.pgsa_utils as pgu
#'''
#Interface to job execution via qsub
#'''

def get_job_submission_args(host_type='standard'):
    cluster_name = xu.get_value_of_key(system_config_root, 'cluster_name')
    qsub_group_list=xu.get_value_of_key(system_config_root, 'cluster/job_queue/' + host_type + '/group')
    qsub_q=xu.get_value_of_key(system_config_root, 'cluster/job_queue/'+host_type+'/queue_name')
    qsub_ppn = xu.get_value_of_key(system_config_root, 'cluster/job_queue/'+host_type+'/queue_ppn')
    print("host_type =", host_type, " ", qsub_group_list, qsub_q, qsub_ppn)
    return [qsub_group_list, qsub_q, qsub_ppn] #TODO: this should be dictionary


def get_pgsa_launch_template():
    pgsa_launch_qsub_template=Template(open(dicex_pgsa_dir+"/pgsa.qsub.template").read()) #TODO: put pgsa.qsub.template here
    #safe substitue here and then send
    [qsub_group_list, qsub_q, qsub_ppn]  = get_job_submission_args(host_type)
    return pgsa_launch_qsub_template


def launch_job(launchjob_script_fn):
    FNULL = open(os.devnull, 'w')
    subprocess.call("qsub "+launchjob_script_fn,stdout=FNULL, shell=True)
    return None


def get_pgsa_cleanup_template():
    pgsa_cleanup_qsub_template = Template(open(dicex_pgsa_dir+"/pgsa_cleanup.qsub.template").read())
    return pgsa_cleanup_qsub_template


def launch_cleanup_job(pgsa_cleanup_script_fn):
    post_cleanup_job_cmd = Template("qsub -W depend=afterany:$jobid ${pgsa_cleanup_cleanup_fn}").substitute(locals())
    subprocess.call(post_cleanup_job_cmd, stdout=FNULL, shell=True)

def removejob(dbdesc):
    set_dbdesc(dbdesc)
    exec_remote_cmd(dbdesc, "cleandb")
    jobid=pgu.get_jobid(dbdesc)
    if jobid is not None:
        subprocess.call("qdel "+jobid, shell=True)
    else:
        print("Error in remove job")
