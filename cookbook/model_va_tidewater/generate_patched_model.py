#generate grid_id, location for ode in python/postgres
#generate ef location-activity file
import sys
import versa_api as vapi
import versa_impl as vi
session = vi.init(sys.argv[1])
try:
    session.execute("create extension postgis")
    session.commit()
except:
    print "post gis already exists"
    session.rollback()

#load the grided locations
from sub_ll import *
#copy out grid-has-location data
#vapi.export_rmo(session, c_sub_ll, model_name='grid_locations')

###
#create uel (src, label, desc, label, duration)
###
from act_by_daysegs import *

#manual mode
stmt = session.query(c_act_by_daysegs).subquery()

#order the fields to match epifast uel format
stmt = session.query(getattr(stmt.c, 'location'), getattr(stmt.c, 'seq_id').label('src_label'), getattr(stmt.c, 'pid'), getattr(stmt.c, 'seq_id').label('des_label'), getattr(stmt.c, 'sum_dseg_duration')).subquery()

#export as uel
vapi.export_rmo(session, stmt, model_name='location_person_segact', csv_fn='location_person_segact.csv', delimiter=' ')



