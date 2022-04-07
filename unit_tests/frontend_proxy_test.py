# server part
#from jobmanagers.jobmanager import get_jobmanager
#import controller.frontend_controller as fcntrl

import rpyc
import versa_engine as ve
jobmanager = ve.get_jobmanager()
fcntrl.launch_frontend_proxy(jobmanager, 5669)

# client part
conn = rpyc.connect("localhost", 5669)
