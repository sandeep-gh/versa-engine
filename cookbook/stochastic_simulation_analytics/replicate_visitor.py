import copy
import getpass

from string import Template
import epistudy_cfg as ec
import pgsa_utils as pgu
import subprocess
import pgsa_utils as pgu
import socket
import versa_impl as vi
import metadata_utils as mu
import xmlutils as xu ##for debugging only
import versa_utils as vu
import versa_api_plot as vapip
import versa_api as vapi
import versa_api_analysis as vapia
import import_header as ih
import replicate_analysis_utils as rau

def dummy_method(cfg_root=None, param_root=None, replicate_desc=None, rep_dir=None, args=None):
    print rep_dir
    return



# def plot_debug(session, model_obj1, model_obj2):
#     agg_by_day1 = vapi.limit(session, vapia.build_stmt_X_vs_agg_Y(session = session, model = model_obj1,  agg_by= 'day', agg_on= 'infections', agg_label='daily_infection_count'), 100)
#     agg_by_day2 = vapi.limit(session, vapia.build_stmt_X_vs_agg_Y(session = session, model = model_obj2,  agg_by= 'day', agg_on= 'infections', agg_label='daily_infection_count'), 100)
#     print vapi.scan(session, agg_by_day1)
#     print vapi.scan(session, agg_by_day2)
#     union_stmt = vapi.set_union(session, agg_by_day1, agg_by_day2, keep_duplicates='True', preserve_column_name=True)

#     union_stmt_by_day = vapi.ascending(session, union_stmt, 'day')
#     print vapi.scan(session, union_stmt_by_day)
#     stmt = vapi.limit(session, union_stmt_by_day, 40)
#     print vapi.scan(session, stmt)
    


        

def visit_replicates(cfg_root=None, param_desc_prefix='', param_root=None, curr_dir=None, method=None, args=None):
    '''
    the method to be applied to each replicate
    '''
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
        method(cfg_root=cfg_root, param_root=param_root, replicate_desc=param_desc_prefix[:-1], rep_dir=curr_dir, args=args)
        return

    for child_param_val in child_param_values:
        child_param_dir=base_dir + '/param_'+ child_param_name + "_" + str(child_param_val)
        subprocess.call('mkdir -p ' + child_param_dir, shell=True)
        curr_cfg_root=copy.deepcopy(cfg_root)
        ec.add_cfg_node(curr_cfg_root, child_param_name, child_param_val)
        ec.set_cfg_node(curr_cfg_root, child_param_name, child_param_val)
        child_desc_prefix=param_desc_prefix +  child_param_abbrv + '_' + str(child_param_val).replace(".", "") + '_'
        visit_replicates(cfg_root=curr_cfg_root, param_desc_prefix=child_desc_prefix, param_root=child_param_root, curr_dir=child_param_dir, method=method, args=args)


def get_all_replicate_models(session, study_cfg_root, model_base_name):
    class replicate_models:
        def __init__(self):
            self.all_models = []
        def add_models(self, cfg_root=None, param_root=None, replicate_desc=None, rep_dir=None, args=None):
            rep_model = rau.get_replicate_model_obj(replicate_desc, model_base_name)
            self.all_models.append(rep_model)
    rm = replicate_models()
    add_models_func = (lambda cfg_root=None, param_root=None, replicate_desc=None, rep_dir=None, args=None: rm.add_models(cfg_root, param_root, replicate_desc, rep_dir, args))

    visit_replicates(study_cfg_root, method=add_models_func, args=[session])
    return rm.all_models

