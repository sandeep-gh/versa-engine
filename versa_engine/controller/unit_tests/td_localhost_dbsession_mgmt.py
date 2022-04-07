# server part
import rpyc
import versa_engine as ve
#from jobmanagers.jobmanager import get_jobmanager
#import controller.frontend_controller as fcntrl


jobmanager = ve.get_jobmanager()
#fcntrl.launch_frontend_proxy(jobmanager, 5669)

# client part
#conn = rpyc.connect("localhost", 5669)
session_name = "ecntrl_test"
proxy_conn_ctx = ve.launchdbjob(dbdesc=session_name)
