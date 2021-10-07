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
session = vapiu.build_session('ap3')

#load data
[person,airline_monthly_flow,region_label_id,us_metros_region_person,us_metros_region_popsz,region_label_id,us_metros_region_person,us_metros_region_popsz]= [vu.import_model(model_name) for model_name in ['person','airline_monthly_flow', 'region_label_id','us_metros_region_person','us_metros_region_popsz','region_label_id','us_metros_region_person','us_metros_region_popsz']]

#convert lat lon to coordinate
airline_monthly_flow_coord = llg.add_point_geom_col(session, airline_monthly_flow, "origin_lon", "origin_lat", "origin_coord")
airline_monthly_flow_coord = llg.add_point_geom_col(session, airline_monthly_flow_coord, "dest_lon", "dest_lat", "dest_coord")
person_coord = llg.add_point_geom_col(session, person, "homex", "homey", "origin_coord")

#join rid to the population
rid_person_coord = vajl.fuseEQ(session, person_coord, us_metros_region_person,['pid'],['pid'])

#Probably will need to materialize this
rid_area_table = vra.createMultipleBoundingBox(session,rid_person_coord,'homex', 'homey','rid','bb_shape')
rid_origin_area_table = vapi.alias_attributes(session, rid_area_table, alias_prefix='origin')
rid_dest_area_table = vapi.alias_attributes(session, rid_area_table, alias_prefix='dest')

#fuses
airline_rid_origin=vra.fuseCoversUnique(session,rid_origin_area_table,airline_monthly_flow_coord,'origin_bb_shape','origin_coord',['origin', 'dest', 'month'],counterColumn="counterColumn")
airline_rid_origin_dest=vra.fuseCoversUnique(session,rid_dest_area_table,airline_rid_origin,'dest_bb_shape','dest_coord',['origin', 'dest', 'month'],counterColumn="counterColumn1")

#remove everything we don't need
airline_rid = vapil.proj(session, airline_rid_origin_dest, ['origin_rid','dest_rid','month','passengers'])

#count the number of people going from area to another, per month
final = vapil.aggregate_sum(session, airline_rid,  ['month','origin_rid','dest_rid'], 'passengers', aggLabel='traveling_passenger')

materializeName='final_results'
final_materialize=vapi.materialize_rmo(session, final, materializeName, "class_"+materializeName, "", "cls_"+materializeName, ["month",'origin_rid','dest_rid'])



