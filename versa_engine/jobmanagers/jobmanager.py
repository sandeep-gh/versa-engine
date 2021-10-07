import os
import common.utilities as utilities

jobmanager_module_dir = os.path.dirname(os.path.realpath(__file__))
def get_jobmanager(jobmanager_type = "localhost"):
    return utilities.import_module(jobmanager_module_dir, "localhostjobmanager")
