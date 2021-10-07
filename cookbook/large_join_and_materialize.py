import matplotlib
matplotlib.use('Agg')
import versa_api as vapi
import versa_api_list as vapil
import versa_api_join_list as vajl
import versa_api_utils as vapiu
import versa_utils as vu
import versa_api_meta as  vam
import versa_geom as vg
import versa_api_plot as vapip
import lat_lon_to_geom as llg
import versa_range_api as vra
import versa_geom as vg
from sqlalchemy.sql.expression import cast
from geoalchemy2.types import Geography
#from import_header import *

#TODOs
session = vapiu.build_session('synpop_wash')

#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
#  prepare datasets: load models, pick relevant attributes, rename columns
#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
state_abbrv = 'ca_'
synpop_models = ['person', 'activities', 'household']
#load synpop data for the state
[person, activities, household] = [vu.import_model(model_name) 
                                   for model_name in 
                                   (state_abbrv + vmodel for vmodel in synpop_models)
                                   ]

person =  vapil.proj(session, person, ['pid', 'hid'])
household = vapil.proj(session, household, ['hid', 'state', 'county', 'tract'])
activities = vapil.proj(session, activities, ['pid', 'anum', 'purpose', 'location'])


home_activity = vapi.filterEQ(session, activities, 'purpose', 1)
#append w_ prefix to attributes
home_activity = vapi.alias_attributes(session, home_activity, alias_prefix='h')


work_activity = vapi.filterEQ(session, activities, 'purpose', 2)
#append w_ prefix to attributes
work_activity = vapi.alias_attributes(session, work_activity, alias_prefix='w')

# #~~~:x:~~~:x:~~~:x:~~~:x:~~~:x:~~~:x:~~~:x:~~~:x:~~~:x:~~~:x:~~~:x:



# #::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
# #  joins/links dataset
# #::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

# #join home and work activity
home_and_work = vapi.fuseEQ(session, home_activity, work_activity, 'h_pid', 'w_pid')
#print "home_and_work=", vapi.cardinality(session, home_and_work)


home_and_work = vapi.materialize_rmo(session, 
                                         home_and_work,
                                         tbl_name = 't_home_and_work', cls='c_home_and_work', cls_fn='home_and_work_model.py', reload=True)






print "done materialization; total rows stored = ", vapi.cardinality(session, home_and_work)
