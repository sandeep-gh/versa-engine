#from utilities import system_config_root
#'''
#Interface to job execution on local machine via shell
#'''

jobmanager_dir = os.path.dirname(os.path.realpath(__file__))

def get_job_submission_args(host_type='standard')::
    return None



def get_pgsa_launch_template():
    pgsa_launch_template=Template(open(jobmanager_dir+"/pgsa.sh.template").read()) #TODO: put pgsa.qsub.template here
    #safe substitue here and then send
    #submission_args  = get_job_submission_args(host_type)
    return pgsa_launch_qsub_template


def launch_job(launch_script_fn):
    subprocess.call([launch_script_fn], shell=True, stdout=subprocess.DEVNULL)
    return


def get_pgsa_cleanup_template():
    pgsa_cleanup_template=Template(open(jobmanager_dir+"/pgsa_cleanup.sh.template").read()) #TODO: put pgsa.qsub.template here

    return pgsa_cleanup_template
