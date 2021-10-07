import os
import imp
import versa_api as vapi
import versa_api_list as vapil
import versa_impl as vi
import versa_range_api as vra
from sqlalchemy import func
import sqlalchemy
from sqlalchemy.sql.expression import cast
from range import IntRangeType


#load all activities
from activities_home_model import *
from activities_nonhome_model import *


#attr/columns of the datasets
#activities (PID, ANUM, starttime, duration, location)

session = vi.init(sys.argv[1])


try:
    session.execute("create extension postgis")
except:
    session.rollback()
    print "post gis already exists"



#divide the day into segments; we can have at most 5 segments of the day
segments_of_day = [[0,21600],[21600, 43200], [43200, 64800], [64800,86400]]
#the label for each segment
segment_labels = ['morning', 'afternoon', 'evening', 'night']

#union home and nonhome activities in one
all_activities = vapi.set_union_rmo_pair(session,  
                                         rmoA=vapil.proj(session, activities_home, ['hid', 'pid', 'anum', 'purpose', 'starttime', 'duration', 'location']),
                                         rmoB=vapil.proj(session, activities_nonhome, ['hid', 'pid', 'anum', 'purpose', 'starttime', 'duration', 'location']),
                                         preserve_column_name=True
                                         )


# append duration = [starttime, (starttime +duration] to the activities tables 
# doing it manually -- versa has no api for this currently
stmt = session.query(all_activities, (getattr(all_activities.c, 'starttime') + getattr(all_activities.c, 'duration')).label('endtime')).subquery()

#filter negative activites
stmt = vapi.filterAttrLTE(session=session, rmo=stmt,  attr1='starttime', attr2='endtime')


#create range = [starttime, endtime] -- needed for intersection with time segments
#again manually -- versa has no api for this as well -- don't mind the messiness
stmt = session.query(stmt, cast(func.concat('[', cast(getattr(stmt.c, 'starttime'), sqlalchemy.String), ',' , cast(getattr(stmt.c, 'endtime'), sqlalchemy.String) , ']'), IntRangeType).label('act_duration')).subquery()

#project the necessary attributes
stmt = vapil.proj(session, stmt, ['pid', 'anum', 'location', 'act_duration'])

#materialize the augmented activites -- for performance reasons
try:
    sys.path.append(os.getcwd())
    imp.find_module('act_range')
    act_range=__import__('act_range')
    act_range = getattr(act_range, 'act_range') #ignore this line for now--workaround to load module
    print "using already computed activity with range"
except Exception, e:
    print "did not find precompute act_range ", str(e)
    print "augment activities with range = [starttime, endtime].."
    [t_act_range, c_act_range, act_range] = vapi.materialize_rmo(session, stmt, tbl_name='t_act_range', cls='act_range', indexes=['pid', 'anum',  'locationid', 'act_duration'], cls_fn='act_range.py', pks=['pid', 'anum'])
    print "augment activities with range = [starttime, endtime]..done"


# create table for day segements and their labels; TODO: we also need to assign a ID
#the table has attributes [seq_id, time_period_range, time_period_name]
try:
    sys.path.append(os.getcwd())
    imp.find_module('c_day_segments')
    c_day_segments=__import__('c_day_segments')
    c_day_segments= getattr(c_day_segments, 'c_day_segments')
    print "using already computed day segments"
except Exception, e:
    print "did not find precomputed day segments ", str(e)
    c_day_segments = vra.createTimePeriod(session, ranges=segments_of_day, labels=segment_labels, tableName='t_day_segments', clsName = 'c_day_segments', clsFn='c_day_segments.py')



# intersect activity duration with day segments
stmt = vra.fuseRangeOverlaps(session, act_range, c_day_segments, 'act_duration', 'time_period_range', preserve_columns=True)

#chop activity by daysegments
stmt = vra.rangeIntersect_attrs(session, stmt, 'act_duration', 'time_period_range', intersect_label='act_dseg')

#augument activity duration by daysegments 
stmt = vra.appendRangeLength(session, stmt, 'act_dseg', 'dseg_duration')


#proj necessary column 
#pid, seq_id (the id of the day segment), location, act_dseg (the range of the activity during that session)
stmt = vapil.proj(session, stmt, ['pid', 'seq_id', 'location',  'dseg_duration'])

#sum the activity duration 
stmt = vapil.aggregate_sum(session, stmt, sum_on_attr='dseg_duration', sum_by_attr=['pid', 'seq_id', 'location'], aggLabel='sum_dseg_duration')

print vapi.cardinality(session, stmt)
#save activity segmented by day period
print "split activity duration by day segments..."
[t_act_n_daysegs, c_act_n_daysegs, act_n_daysegs] = vapi.materialize_rmo(session, stmt,  tbl_name = 't_act_by_daysegs', cls='c_act_by_daysegs',  indexes = ['pid', 'location', 'seq_id', 'act_dseg'], cls_fn='act_by_daysegs.py', pks=['pid', 'location', 'seq_id', 'sum_dseg_duration'])
print "split activity duration by day segments...done"

#this is a todo
#vapi.export_rmo(session, rmo, 'act_by_day_

session.commit()

