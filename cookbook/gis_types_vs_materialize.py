import matplotlib
matplotlib.use('Agg')
import sys
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
session_name = sys.argv[1]
session = vapiu.build_session(session_name)

#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
#  prepare datasets: load models, pick relevant attributes, rename columns
#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
#dnb, tract, acs survery
[dnb, census_tract] = [vu.import_model(model_name) for model_name in ['us_dnb', 'census_tract']]

dnb = vapil.proj(session, dnb, ['id', 'latitude', 'longitude'])
census_tract = vapil.proj(session, census_tract, ['statefp00', 'countyfp00', 'tractce00', 'geom'])


#fix the leading zero mismatch for fips code
#household has no leading zeros, but census_tract has
census_tract = vapil.remove_leading_zeros(session, census_tract, ['statefp00', 'countyfp00', 'tractce00'])
#tried the option of geom to geography for materialization
#census_tract = vapi.rename_attribute(session, census_tract, 'geom', 'p_geom')
#census_tract = session.query(census_tract, cast(getattr(census_tract.c, 'p_geom'), Geography).label('geom')).subquery()

census_tract = vg.get_centroid_geom(session, census_tract, 'geom', 'centroid')

#unit test 1: materializatin  of geom and derived coord 
census_tract = vapi.materialize_rmo(session, census_tract, tbl_name='t_census_tract', cls='c_census_tract', cls_fn='census_tract_derived_model.py', pks=['statefp00', 'countyfp00', 'tractce00'], indexes=['statefp00', 'countyfp00', 'tractce00'],  reload=True)


dnb = llg.add_point_geom_col(session, dnb,  'longitude', 'latitude','coord')
dnb = vra.fuseContains(session, census_tract, dnb, 'geom', 'coord')
dnb = vapil.proj(session, dnb, ['id', 'statefp00', 'countyfp00', 'tractce00'])

#unit test 2: derived lat/lon coordinate, fuseContains and materialize
dnb = vapi.materialize_rmo(session, dnb,
                           tbl_name = 't_dnb', cls='c_dnb', cls_fn='dnb_model.py', reload=True)


