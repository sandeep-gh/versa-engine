import versa_utils as vu
from itertools import izip
import versa_api as vapi
def pairwise(iterable):
    "s -> (s0,s1), (s2,s3), (s4, s5), ..."
    a = iter(iterable)
    return izip(a, a)

def get_replicate_model_obj(replicate_desc, model_base_name):
    model_name = replicate_desc + model_base_name
    model_obj = vu.import_model(model_name)
    return model_obj


def annotate_model_param_sweep_attrsvals(session, cls):
    cls_name = cls.__name__
    rep_cfg  = cls_name.split('_r_')[0]
    for par,val in pairwise(rep_cfg.split('_')):
        cls = vapi.add_const_column(session, cls, val, par)
    return cls

def annotate_models_param_sweep_attrsvals(session, models):
    return [annotate_model_param_sweep_attrsvals(session, m) for m in models]

