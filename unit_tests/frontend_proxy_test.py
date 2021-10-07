# server part
from jobmanagers.jobmanager import get_jobmanager
import controller.frontend_controller as fcntrl

jobmanager = get_jobmanager()
fcntrl.launch_frontend_proxy(jobmanager, 5669)

# client part
import rpyc
conn = rpyc.connect("localhost", 5669)
