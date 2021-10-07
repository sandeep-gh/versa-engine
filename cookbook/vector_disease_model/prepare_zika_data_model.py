#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#The program builds a zika data model using the synthetic population (person,
#households, activities, school, and workplace). In the resulting model 
#activities are divided into day segements. Currently, this program uses four 
#day segements: morning, afternoon, evening, and night. For realistic models this
#may change.
#
#The input is a rectangular region of a country specified by lower left latitude
#(ll_lat), upper right latitude (ur_lat), lower left longitude (ll_lon), and 
#upper right longitude (ur_lon).
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#Before starting this program -- run setup to load syn pop data
#run -i ~/NDSSL-Software/PopulationPipelineTestingAndAnalysis/setup_test.py gp_bra BRA_workflow_log.xml
#this may take some time

import versa_fishnet_api as vfa
import lat_lon_to_geom as llg
import versa_api as vapi
import versa_api_list as vapil
import versa_impl as vi
import versa_range_api as vra
from sqlalchemy import func
import sqlalchemy
from sqlalchemy.sql.expression import cast
from range import IntRangeType


import sys
region='brazil_salvador_subregion'
ll_lat = -23.686995
ur_lat = -23.408109
ll_lon = -47.360013
ur_lon = -45.696085

#divide the day into segments; we can have at most 5 segments of the day
segments_of_day = [[0,21600],[21600, 43200], [43200, 64800], [64800,86400]]

#the label for each segment
segment_labels = ['morning', 'afternoon', 'evening', 'night']

#the grid size of the fishnet in kilometers
grid_sz = 1

incubation_period = 7
infectious_period = 7

session = vi.init(sys.argv[1])
try:
    session.execute("create extension postgis")
    session.commit()
except:
    print "post gis already exists"


# create table for day segements and their labels; TODO: we also need to assign a ID
#the table has attributes [seq_id, time_period_range, time_period_name]
day_segments = vra.createTimePeriod(session, ranges=segments_of_day, labels=segment_labels, tableName='t_day_segments', clsName = 'c_day_segments')



#define geometery rectangle  for the subregion
subregion_bb = vfa.bb_to_geom(ll_lat=ll_lat, ur_lat=ur_lat, ll_lon=ll_lon, ur_lon=ur_lon) 

#create a grid of 1km by 1km over the rectangle
fishnet = vfa.createFishNetTable(session, region+'_grid', region +'_grid', subregion_bb, grid_sz, grid_sz) 



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# households
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
from households_model import *

#append households with point geometry --useful for filtering by region (spatial rectangles).
#_f in hh_c_f stands for functional, i.e., the coord attribute is a function, in this case over latitude and 
#longitude

hh_c_f = vapil.proj(session, llg.add_point_geom_col(session, households, 'latitude', 'longitude', 'coord'),
                  ['locationid', 'coord']) 

#materialize hh_c_f and index on coord -- indexing should help with scalability
[t_hh_c, c_hh_c, hh_c] = vapi.materialize_rmo(session, hh_c_f, tbl_name='t_hh_c', cls='c_hh_c', indexes=['locationid', 'coord'],  cls_fn='hh_c.py', pks=['locationid'])

#mesh the households with grid, i.e, find which grid contains which household;
#households outside the grids are dropped
#use the intervalFuse code to test performance 
fishnet_sub_hh_c = vfa.fuseContains(session, fishnet, hh_c, 'fishnet', 'coord')

#define the fid-to-locationid relationship and materialize it
fishnet_sub_hh = vapil.proj(session, fishnet_sub_hh_c, ['fid', 'locationid'])
[t_sub_hh, c_sub_hh, sub_hh] = vapi.materialize_rmo(session, fishnet_sub_hh, tbl_name='t_sub_hh', cls='c_sub_hh', indexes=['locationid', 'coord'],  cls_fn='sub_hh.py', pks=['locationid'])
 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# mesh workplaces
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#append coord attribute for workplaces, materialize it and index it by coord. 
from workplace_model import *
wp_c_f = vapil.proj(session, llg.add_point_geom_col(session, workplace, 'latitude', 'longitude', 'coord'),
                    ['locationid', 'coord'])
[t_wp_c, c_wp_c, wp_c] = vapi.materialize_rmo(session, wp_c_f, tbl_name='t_wp_c', cls='c_wp_c', indexes=['locationid', 'coord'],  cls_fn='wp_c.py', pks=['locationid'])

fishnet_sub_wp_c = vfa.fuseContains(session, fishnet, wp_c, 'fishnet', 'coord')
fishnet_sub_wp = vapil.proj(session, fishnet_sub_wp_c, ['fid', 'locationid'])
[t_sub_wp, c_sub_wp, sub_wp] = vapi.materialize_rmo(session, fishnet_sub_wp, tbl_name='t_sub_wp', cls='c_sub_wp', indexes=['locationid', 'coord'],  cls_fn='sub_wp.py', pks=['locationid'])


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# mesh schools
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#append coord attribute for workplaces, materialize it and index it by coord. 
#Similarly, append coord attribute for schools, materialize it and index it by coord. 
from school_model import *
sp_c_f = vapil.proj(session, llg.add_point_geom_col(session, school, 'latitude', 'longitude', 'coord'),
                    ['locationid', 'coord'])
[t_sp_c, c_sp_c, sp_c] = vapi.materialize_rmo(session, sp_c_f, tbl_name='t_sp_c', cls='c_sp_c', indexes=['locationid', 'coord'],  cls_fn='sp_c.py', pks=['locationid'])
fishnet_sub_sp_c = vfa.fuseContains(session, fishnet, sp_c, 'fishnet', 'coord')
fishnet_sub_sp = vapil.proj(session, fishnet_sub_sp_c, ['fid', 'locationid'])
[t_sub_sp, c_sub_sp, sub_sp] = vapi.materialize_rmo(session, fishnet_sub_sp, tbl_name='t_sub_sp', cls='c_sub_sp', indexes=['locationid', 'coord'],  cls_fn='sub_sp.py', pks=['locationid'])


#collect meshing household, workplace, and school as one object
all_fid_lid = vapi.set_union(session, [sub_hh, sub_wp, sub_sp], preserve_column_name=True)
[t_sub_loc, c_sub_loc, sub_loc] = vapi.materialize_rmo(session, all_fid_lid, tbl_name='t_sub_loc', cls='c_sub_loc', indexes=['fid', 'locationid'], cls_fn='sub_loc.py', pks=['locationid'])



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Find all activities happening at the sublocation 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#Using direct sql due to performance reasons. Needs some debugging here
#TODO: double check this; ideal would use materialize rmo approach 

from activities_model import *
#find all activities happening at locations within the fishnet
sub_acts_stmt = vapi.fuseEQ(session, all_fid_lid, activities, 'locationid', 'f_locationid')
[t_sub_acts, c_sub_acts, sub_acts] = vapi.materialize_rmo(session, sub_acts_stmt, tbl_name = 't_sub_acts', cls='c_sub_acts', indexes=['fid', 'f_pid', 'f_locationid', 'anum'], cls_fn='sub_act.py', pks=['fid', 'f_pid', 'f_locationid', 'anum'])
#session.execute("create table sub_act as select f_pid, f_locationid, starttime, duration, anum from activities, t_sub_loc where f_locationid=locationid")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# append duration = [starttime, (starttime +duration] to the activities tables
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#from sub_act import *
#act_subloc = vapi.select(session, c_sub_act)
sub_act_w_et = session.query(sub_acts, (getattr(sub_acts.c, 'starttime') + getattr(sub_acts.c, 'duration')).label('endtime')).subquery()

sub_act_w_range = session.query(sub_act_w_et, cast(func.concat('[', cast(getattr(sub_act_w_et.c, 'starttime'), sqlalchemy.String), ',' , cast(getattr(sub_act_w_et.c, 'endtime'), sqlalchemy.String) , ']'), IntRangeType).label('act_duration')).subquery()
sub_act_s = vapil.proj(session, sub_act_w_range, ['f_pid', 'anum', 'f_locationid', 'act_duration'])
[t_sub_act_range, c_sub_act_range, sub_act_range] = vapi.materialize_rmo(session, sub_act_s, tbl_name='t_sub_act_range', cls='sub_act_range', indexes=['f_pid', 'anum',  'f_locationid', 'act_duration'], cls_fn='sub_act_range.py', pks=['f_pid', 'anum'])




#find which activities 
act_n_daysegs = vra.fuseRangeOverlaps(session, sub_act_range, day_segments, 'act_duration', 'time_period_range', preserve_columns=True)
act_by_daysegs = vra.rangeIntersect_attrs(session, act_n_daysegs, 'act_duration', 'time_period_range', intersect_label='act_dseg')

act_by_daysegs_s = vapil.proj(session, act_by_daysegs, ['f_pid', 'seq_id', 'f_locationid',  'act_dseg'])
[t_act_n_daysegs, c_act_n_daysegs, act_n_daysegs] = vapi.materialize_rmo(session, act_by_daysegs_s,  tbl_name = 't_act_by_daysegs', cls='c_act_by_daysegs',  indexes = ['f_pid', 'f_locationid', 'seq_id', 'act_dseg'], cls_fn='act_by_daysegs', pks=['f_pid', 'f_locationid', 'time_period_name', 'act_dseg'])



#the rest of the modeling is in direct sql because not able 
#to find the span of a duration using Versa_ranges
#conn = session.connection()
#engine = cc.engine
#engine.execute("copy (select fid, locationid, pid, atype, anum, time_period_name, (upper(act_dseg_duration) - lower(act_dseg_duration)) as duration from  t_act_by_dseg_final) to '/localscratch/sandeep/grid_dseg_hh_act.dat' delimiter '|';")


# alter table t_day_segments ADD column time_period_id int;
# update t_day_segments set time_period_id = 4 where time_period_name = 'night';
# update t_day_segments set time_period_id = 3 where time_period_name = 'evening';
# update t_day_segments set time_period_id = 2 where time_period_name = 'afternoon;
# update t_day_segments set time_period_id = 1 where time_period_name = 'morning';
# copy (select * from t_sub_loc) to '/localscratch/sandeep/fid_locationid.txt' delimiter ' ';
# copy (select distinct(f_pid) from t_act_by_daysegs) to '/localscratch/sandeep/sub_persons.txt' ;
# copy (select O.f_locationid, S.time_period_id, O.f_pid, S.time_period_id, (upper(O.act_dseg) - lower(O.act_dseg)) from t_act_by_daysegs as O, t_day_segments as S where O.time_period_name = S.time_period_name) to '/localscratch/sandeep/sub_act_dseg.uel' delimiter ' ';

#copy grid_locations from '/groups/NDSSL/studies/zika_2016/sp_sample/grid_locations.dat' delimiter ' ';
#create table lid_pid as  (select lid, pid from (select distinct f_locationid as lid, f_pid as pid  from sub_act) AS O);
#create table fid_popsz as (select fid, count(pid) as popsz from grid_locations as gl, lid_pid as lp where gl.lid = lp.lid group by fid);
