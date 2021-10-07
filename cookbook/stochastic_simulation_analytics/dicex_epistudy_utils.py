import copy
import getpass
from string import Template
import epistudy_cfg as ec
import pgsa_utils as pgu
import subprocess
import random 
import pgsa_utils as pgu
import socket
import versa_impl as vi
import metadata_utils as mu
import utils
import os
import xmlutils as xu ##for debugging only
import sys
import imp
user=getpass.getuser()
module_dir=os.path.dirname(os.path.realpath(__file__))

def init(cfg_root):
    dbsession = ec.get_db_session(cfg_root)
    vi.init(dbsession)
    base_dir=ec.get_study_base_dir(cfg_root)
    subprocess.call('mkdir -p ' + base_dir, shell=True)

def update_intv_files(cfg_root, replicate_desc, replicate_dir, param_root):
    '''update the intervention file with the indemics server ip address
    '''
    indemics_server_hostname=socket.gethostname()
    indemics_server_ipaddr=socket.gethostbyname(indemics_server_hostname)

    efdir=replicate_dir + '/epifast_job'
    intv_config_loc = efdir+'/Intervention'
    intv_config_template_loc = intv_config_loc + '.template'
    intv_config_template_str = open(intv_config_template_loc, 'r').read()
    intv_config_template = Template(intv_config_template_str)
    a = locals()
    b = globals()
    a.update(b)
    intv_config_str = intv_config_template.substitute(a)
    intv_cfg_fh = open(intv_config_loc, 'w')
    intv_cfg_fh.write(intv_config_str)
    intv_cfg_fh.close()


def prepare_config_files(cfg_root, replicate_desc, replicate_dir, param_root):
    replicate_id = ec.get_replicate_id(cfg_root)
    template_dir = ec.get_cfg_template_dir(cfg_root)
    #intv_type = ec.get_intv_type(cfg_root)
    iql_template_loc = ec.get_iql_loc(cfg_root)
    assert (iql_template_loc is not None)
    socnet_path_template = ec.get_socnet_path_template(cfg_root)
    
    study_region = ec.get_study_region(cfg_root).lower()
    ef_socnet_path = Template(socnet_path_template).substitute(locals())

    dbsession=ec.get_dbsession(cfg_root)
    coord_ip_address=pgu.get_db_host(dbsession)
    efdir=replicate_dir + '/epifast_job'
    subprocess.call('mkdir -p ' + efdir, shell=True)
    print efdir
    rand_gen = random.randint(1000000, 9000000)
    print "seed = ", rand_gen
    epifast_config_template_loc = template_dir + '/efcfg.template'
    epifast_config_loc = efdir + '/Configuration'
    epifast_config_template_str=open(epifast_config_template_loc, "r").read()
    epifast_config_template= Template(epifast_config_template_str)
    epifast_settings_dict = ec.get_epifast_setting(cfg_root)
    a = locals()
    b = globals()
    a.update(b)
    a.update(epifast_settings_dict)
    epifast_config_str = epifast_config_template.substitute(a)
    efcfg = open(epifast_config_loc, 'w+')
    efcfg.write(epifast_config_str)
    efcfg.close()     
    
    ### prepare Diagnosis file
    diagnosis_config_loc = efdir + '/Diagnosis'
    diagnosis_fh =  open(diagnosis_config_loc, 'w+')
    diagnosis_dict = ec.get_epifast_diagnosis_settings(cfg_root)
    diagnosis_fh.write("ModelVersion = " + diagnosis_dict['ModelVersion']+'\n')
    diagnosis_fh.write("ProbSymptomaticToHospital = " + diagnosis_dict['ProbSymptomaticToHospital']+'\n')
    diagnosis_fh.write("ProbDiagnoseSymptomatic = " + diagnosis_dict['ProbDiagnoseSymptomatic']+'\n')
    diagnosis_fh.write("DiagnosedDuration = " + diagnosis_dict['DiagnosedDuration']+'\n')
    diagnosis_fh.close()
    

    ##prepare intervention file
    indemics_server_ipaddr = '${indemics_server_ipaddr}' #to be filled by indemics server
    intv_config_template_loc = template_dir + '/intvcfg.template'
    intv_config_loc = efdir+'/Intervention.template'
    intv_config_template_str = open(intv_config_template_loc, 'r').read()
    intv_config_template = Template(intv_config_template_str)
    a = locals()
    b = globals()
    a.update(b)
    intv_config_str = intv_config_template.substitute(a)
    intv_cfg_fh = open(intv_config_loc, 'w')
    intv_cfg_fh.write(intv_config_str)
    intv_cfg_fh.close()

    #prepare iql file
    interventions = ec.get_intervention_param_values(cfg_root)
    all_intv_dict = {}
    for intv_set_key in interventions.keys():
        intv_dict = {}
        assert('iql_key' in interventions[intv_set_key].keys())
        iql_key = interventions[intv_set_key]['iql_key']
        for intv_attr in [x for x in interventions[intv_set_key].keys() if x != 'iql_key']:
            intv_attr_val = interventions[intv_set_key][intv_attr]
            intv_dict[iql_key + '_' + intv_attr] = intv_attr_val
        all_intv_dict.update(intv_dict)
        print intv_dict

#         iql_key = interventions[intv_set_key]['iql_key']
#         type = interventions[intv_set_key]['type']
#         compliance = interventions[intv_set_key]['compliance']
#         duration = interventions[intv_set_key]['duration']
#         delay = interventions[intv_set_key]['delay']
#         efficacy_in = interventions[intv_set_key]['efficacy_in']
#         efficacy_out = interventions[intv_set_key]['efficacy_in']
#         intv_dict = {iql_key+ '_compliance': compliance,
#                      iql_key+ '_duration': duration, 
#                      iql_key+ '_delay': delay,
#                      iql_key+ '_eff_in': efficacy_in,
#                      iql_key+ '_eff_out': efficacy_out,
#                      iql_key+ '_type': type
#                      }
#        all_intv_dict.update(intv_dict)
    i='$i'
    a = locals()
    b = globals()
    a.update(b)
    a.update(all_intv_dict)
    #iql_template_loc = template_dir + '/iql_' + intv_type + '.template'
    iql_loc = efdir + '/intv.iql'
    try:
        iql_template_str = open(iql_template_loc, 'r').read()
    except IOError:
        print("intervention template file not found " + iql_template_loc)

    iql_template = Template(iql_template_str)
    iql_str = iql_template.substitute(a)
    iql_fh = open(iql_loc, 'w+')
    iql_fh.write(iql_str)
    iql_fh.close()
    subprocess.call('chmod +x '+iql_loc, shell=True)

    ########################
    ##Build replicate intv tables
    ###############################
    pre_intv_elem = ec.get_pre_intv_elem(param_root)
    process_pre_intv_begin(replicate_desc, pre_intv_elem)

    return

def process_pre_intv_begin(replicate_desc, pre_intv_node):
    create_table_elems = xu.get_elems(pre_intv_node, 'create_table')
    for ct in create_table_elems:
        metadata_fn = xu.get_value_by_attr(ct, 'metadata')
        location_fp = xu.get_value_by_attr(ct, 'location')
        metadata_root = mu.read_metadata(metadata_fn)
        model_name = mu.get_model_name(metadata_root)
        new_model_name = replicate_desc + '_' + model_name
        mu.set_model_name(metadata_root, new_model_name)
        vi.build_orm_from_metadata(metadata_root, create_table=True, force_create_model=True, location=location_fp, class_decorator="@deco_param_sweep")
        print mu.get_model_name(metadata_root)
    



def launch_ef_jobs(cfg_root, replicate_desc, replicate_dir):
    walltime = ec.get_epifast_walltime(cfg_root)
    template_dir=ec.get_cfg_template_dir(cfg_root)
    indemics_dir = ec.get_indemics_dir(cfg_root)
    study_base_dir = ec.get_study_base_dir(cfg_root)
    region = ec.get_study_region(cfg_root)
    ef_nodes = ec.get_epifast_num_nodes(cfg_root)
    PBS_NODEFILE = '$PBS_NODEFILE'
    SIMDM_ROOT='$SIMDM_ROOT'
    CLASSPATH='$CLASSPATH'
    SIMDM_LIB='$SIMDM_LIB'
    SIMDM_JAR='$SIMDM_JAR'
    NUM_NODES='$NUM_NODES'
    MPIRUN='$MPIRUN' 
    iql_loc = replicate_dir + '/epifast_job/intv.iql'
    ef_cfg_loc = replicate_dir + '/epifast_job/Configuration'
    rep_home = replicate_dir
    job_out_file = rep_home + '/epifast_job/ef_job.out'
    client_out = rep_home  +  '/client.out'
    client_err = rep_home  +  '/client.err'
    ef_out = rep_home + '/epifast_job/ef_out.txt'
    ef_err = rep_home + '/epifast_job/ef_err.txt'
    ef_jobname=region + '-' + replicate_desc
    ef_qsub_job_cfg_template_loc = template_dir + '/epifast_job.qsub.template'
    ef_qsub_job_cfg_loc = rep_home + '/epifast_job/ef_job.qsub'
    ef_qsub_job_cfg_template_str = open(ef_qsub_job_cfg_template_loc, 'r').read()
    ef_qsub_job_cfg_template = Template(ef_qsub_job_cfg_template_str)
    a = locals()
    b = globals()
    a.update(b)
    ef_qsub_job_cfg_str = ef_qsub_job_cfg_template.substitute(a)
    ef_qsub_job_cfg_fh = open(ef_qsub_job_cfg_loc, 'w')
    ef_qsub_job_cfg_fh.write(ef_qsub_job_cfg_str)
    ef_qsub_job_cfg_fh.close()
    subprocess.call('qsub ' + ef_qsub_job_cfg_loc, shell=True) #TODO: open up once everything is rewired
    return

def param_sweep_epifast_job(cfg_root, param_desc_prefix='', param_root=None, curr_dir=None):
    if curr_dir is None:
        base_dir=ec.get_study_base_dir(cfg_root)
    else:
        base_dir = curr_dir

    if param_root is None:
        param_root = cfg_root 

    [child_param_root, child_param_name, child_param_values, child_param_abbrv]  = ec.get_child_sweep_param(param_root)
    param_name=xu.get_value(xu.get_elems(param_root, 'parameter')[0])
    if child_param_root is None:
        assert(param_name=='replicate')
        print curr_dir
        launch_ef_jobs(cfg_root, param_desc_prefix[:-1], curr_dir)
        return

    for child_param_val in child_param_values:
        child_param_dir=base_dir + '/param_'+ child_param_name + "_" + str(child_param_val)
        subprocess.call('mkdir -p ' + child_param_dir, shell=True)
        curr_cfg_root=copy.deepcopy(cfg_root)
        ec.add_cfg_node(curr_cfg_root, child_param_name, child_param_val)
        child_desc_prefix=param_desc_prefix +  child_param_abbrv + '_' + str(child_param_val).replace(".", "") + '_'
        param_sweep_epifast_job(curr_cfg_root, child_desc_prefix, child_param_root, child_param_dir)
        

def vary_param(cfg_root, param_desc_prefix='', param_root=None, curr_dir=None, rep_method=None):
    if curr_dir is None:
        base_dir=ec.get_study_base_dir(cfg_root)
    else:
        base_dir = curr_dir
    if param_root is None:
        param_root = cfg_root 
    [child_param_root, child_param_name, child_param_values, child_param_abbrv]  = ec.get_child_sweep_param(param_root)
    ##TODO: Something is fishy here
    param_name=xu.get_value(xu.get_elems(param_root, 'parameter')[0])
    if child_param_root is None:
        assert(param_name=='replicate')
        rep_method(cfg_root, param_desc_prefix[:-1], curr_dir, param_root)
        #pre_intv_elem = ec.get_pre_intv_elem(param_root)
        #process_pre_intv_begin(param_desc_prefix[:-1], pre_intv_elem)
        return

    for child_param_val in child_param_values:
        child_param_dir=base_dir + '/param_'+ child_param_name + "_" + str(child_param_val)
        subprocess.call('mkdir -p ' + child_param_dir, shell=True)
        curr_cfg_root=copy.deepcopy(cfg_root)
        ec.add_cfg_node(curr_cfg_root, child_param_name, child_param_val)
        ec.set_cfg_node(curr_cfg_root, child_param_name, child_param_val)
        child_desc_prefix=param_desc_prefix +  child_param_abbrv + '_' + str(child_param_val).replace(".", "") + '_'
        vary_param(curr_cfg_root, child_desc_prefix, child_param_root, child_param_dir, rep_method)
        
def create_study_directories(cfg_root):
    random_seed=ec.get_epifast_seed(cfg_root)
    random.seed(random_seed)
    vary_param(cfg_root, rep_method=prepare_config_files)
        

def prepare_launch_indemics_server_script(cfg_root):
    '''
    build launch_indemics_server.sh
    '''

    template_dir=ec.get_cfg_template_dir(cfg_root)
    study_base_dir = ec.get_study_base_dir(cfg_root)
    indemics_dir = ec.get_indemics_dir(cfg_root)
    indemics_server_template_loc = template_dir + '/launch_indemics_server.sh.template'
    indemics_server_loc = study_base_dir + '/launch_indemics_server.sh'
    indemics_server_template_str=open(indemics_server_template_loc, "r").read()
    indemics_server_template= Template(indemics_server_template_str)
    a = locals()
    b = globals()
    a.update(b)
    indemics_server_str = indemics_server_template.safe_substitute(a) #ignore shell variables
    start_fh = open(indemics_server_loc, 'w+')
    start_fh.write(indemics_server_str)
    start_fh.close()
    subprocess.call('chmod +x ' + indemics_server_loc, shell=True)
    
def prepare_indemics_server_cfg(cfg_root):
    max_threads = ec.get_indemics_max_threads(cfg_root) ##TOOD:  used for indemics_server_cfg.template
    template_dir=ec.get_cfg_template_dir(cfg_root)
    study_base_dir = ec.get_study_base_dir(cfg_root)
    dbsession= ec.get_db_session(cfg_root)
    port = pgu.get_db_port(dbsession)
    assert (port is not None)
    coord_hostname=socket.gethostname()
    coord_ip_address=socket.gethostbyname(coord_hostname)
    a = locals()
    b = globals()
    a.update(b)
    conf_dir = study_base_dir + '/conf'
    print "creating conf dir " + conf_dir
    subprocess.call('mkdir -p ' + conf_dir, shell=True)
    
    indemics_server_cfg_template_loc = template_dir + '/indemics_server_cfg.template'
    indemics_server_cfg_loc = study_base_dir+'/conf/simdm.conf'
    indemics_server_cfg_template_str = open(indemics_server_cfg_template_loc, 'r').read()
    indemics_server_cfg_template = Template(indemics_server_cfg_template_str)
    indemics_server_cfg_str = indemics_server_cfg_template.substitute(a)


    indemics_server_cfg_fh = open(indemics_server_cfg_loc, 'w+')
    indemics_server_cfg_fh.write(indemics_server_cfg_str)
    indemics_server_cfg_fh.close()
    
    #prepare simdb_server.conf
    subprocess.call('cp ' + template_dir +  '/simdb.conf ' + study_base_dir + "/conf",  shell=True)
    prepare_launch_indemics_server_script(cfg_root)

def update_replicate_intv_files(cfg_root):
    vary_param(cfg_root, rep_method=update_intv_files)
    


def load_model_data(cfg_root):
    model_data_cfg = ec.get_model_data_cfg_file(cfg_root)
    region = ec.get_study_region(cfg_root)
    
    edcfg_entity_header = Template("""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE edcfg [
<!ENTITY var_region "$region">
<!ENTITY var_metadata_dir "/groups/NDSSL/dicex_metadata_repo/">
]>
""").substitute(locals())
    edcfg_root = xu.read_string(edcfg_entity_header + open(model_data_cfg).read())
    vi.build_orm_files(edcfg_root)

def launch_indemics_server_job(cfg_fn, cfg_root):
    ''' build the qsub file
    '''
    global module_dir
    global user
    template_dir=ec.get_cfg_template_dir(cfg_root)
    study_base_dir = ec.get_study_base_dir(cfg_root)
    dbsession = ec.get_db_session(cfg_root)
    dicex_base_dir =ec.get_dicex_base_dir(cfg_root)
    work_dir= pgu.get_work_dir(dbsession)
    ##################################################################
    ## build indemics_server job
    ##################################################################
    PATH="$PATH"
    PYTHONPATH="$PYTHONPATH"
    PBS_O_WORKDIR="$PBS_O_WORKDIR"
    PBS_JOBID="$PBS_JOBID"
    jobid="$jobid"
    launcher_hostname  = socket.gethostname()
    walltime = xu.get_value_elem(cfg_root, 'indemics_server/walltime')
    print walltime
    message_port = utils.get_new_port()
    template_loc = template_dir + "/indemics_server_job.qsub.template"
    template_str = Template(open(template_loc, 'r').read())
    a = locals()
    b = globals()
    a.update(b)
    cmd_str = template_str.substitute(a)
    qsub_loc = study_base_dir + "/launch_indemics_server.qsub"
    qsub_fh = open(qsub_loc, 'w+')
    qsub_fh.write(cmd_str)
    qsub_fh.close()
    subprocess.call("qsub "+ qsub_loc, shell=True)
    utils.signal_listen(message_port)



def get_indemics_qsub_jobid(dbdesc=None):
    session_fn=pgu.get_session_fn(dbdesc)
    jobid=None
    try:
        sys.path.append(os.getcwd())
        imp.find_module(session_fn)
        dbsession=__import__(session_fn)
        jobid=dbsession.indemics_server_jobid
    except ImportError:
        found=False
        print ("import error in get_qsub_jobid")
    return jobid

