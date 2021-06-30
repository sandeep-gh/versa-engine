import rmo.utils as ru
import dataapis.generators as generators

#submod testing -- ignore
def load_rmos(rmo_names=None, rmo_dir=None):
    """
    load a list of rmos
    
    Args: 
      rmo_names: a list of names of the rmos
      rmo_dir: directory path where the rmo is created. None implies current directory

    
    """
    rmos = [ru.import_model(rmo, model_dir=rmo_dir) for rmo in rmo_names]
    return rmos


def build_ranges(session=None,  prefix=None,  start=0, end=None, step=None):
    return generators.build_ranges_impl(session=session,  seq_label=prefix+"_id", range_lb=prefix+"_lb", range_ub=prefix+"_rb", start=start, step=step, end=end)
