#Readme: this code runs at the begining of ipython to initialize the session

import sys
import os
reload(sys)
sys.setdefaultencoding('utf8')


import sqlalchemy
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker

# from sqlalchemy import Table,Column, Integer, String, Table
# import sqlite3
# from sqlalchemy.orm import aliased
# from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
# from sqlalchemy.orm import Session, relationship, backref,\
#                                 joinedload_all
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm.collections import attribute_mapped_collection

# import scipy.stats as stats
import matplotlib
matplotlib.use('pdf')
import matplotlib.pyplot as plt
import numpy as np

session = None
#TODO: headers will need cleanup at some point in time
import sys
import versa_api as vapi
import versa_impl as vi
import versa_api_utils as vau
import versa_api as vapi
import versa_api_list as vapil
import versa_api_join_list as vajl
import versa_api_utils as vapiu
import versa_utils as vu
import versa_api_meta as  vam
import versa_geom as vg
#import versa_api_plot as vapip
import lat_lon_to_geom as llg
import versa_range_api as vra
import versa_geom as vg
from sqlalchemy.sql.expression import cast
from geoalchemy2.types import Geography

session_run_dir = './'
if 'espresso_session_run_dir' in os.environ:
    session_run_dir = os.environ['espresso_session_run_dir']
    
print "session_run_dir=", session_run_dir                                          
if len(sys.argv) > 1:
    session_name=sys.argv[1]
else:
    session_name = vu.get_latest_dbsession(session_run_dir)
    if session_name is None:
        print("no active dbsession found..exiting")
        sys.exit()
    session = vu.build_session(session_name, session_run_dir=session_run_dir)

print "vi.init"
vi.init(session_name, session_run_dir)
import versa_core.relational as re
import versa_core.export as ex
import versa_core.schema as sc
import versa_core.utils as utils
