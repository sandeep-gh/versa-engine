#Readme: this code runs at the begining of ipython to initialize the session

import sys
import os
#sys.setdefaultencoding('utf8') #this needs fixing


import sqlalchemy
import matplotlib
matplotlib.use('pdf')
import matplotlib.pyplot as plt
import numpy as np

session = None
#TODO: headers will need cleanup at some point in time
import sys
import rmo.versa_impl as vi
import rmo.utils as vu
import rmo.versa_api_meta as  vam
import dataapis.relational as re
import dataapis.ranges.versa_geom as vg
import dataapis.ranges.lat_lon_to_geom as llg
import dataapis.ranges.versa_range_api as vra

from sqlalchemy.sql.expression import cast
from geoalchemy2.types import Geography

session_run_dir = './'
if 'espresso_session_run_dir' in os.environ: #TODO: whats the idea behind espression_session_run_dir
    session_run_dir = os.environ['espresso_session_run_dir']
    
if len(sys.argv) > 1:
    session_name=sys.argv[1]
else:
    session_name = vu.get_latest_dbsession(session_run_dir)
    if session_name is None:
        print("no active dbsession found..exiting")
        sys.exit()
    session = vu.build_session(session_name, session_run_dir=session_run_dir)

vi.init(session_name, session_run_dir)
import dataapis.relational as re
import dataapis.export as ex
import dataapis.schema as sc
import dataapis.utils as utils
