#Readme: This code takes as input the census 2014  shapefiles data (shape of each block,
#and blocks in tract) info and a set of road network vertices as lat/lon. It finds which points
#belong to which tract. If a point is on boundary then an arbitary tract is choosen. 
#Currently some tracts are being left out which is 


import versa_range_api as vra
import versa_geom as vg
from sqlalchemy.sql.expression import cast
from geoalchemy2.types import Geography
from geoalchemy2 import Geometry
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
import versa_apip as vapip
from sqlalchemy.sql.expression import cast
from geoalchemy2.types import Geography
from vertex_geo_model import *

from runner import *

session = vu.build_session(sys.argv[1])
#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
#prepare geom for each tract from tiger shapes
#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

assert session is not None

[census_tract] = vapi.load_rmos(['tiger_tract_2014'])

assert census_tract is not None

census_tract = vapil.remove_leading_zeros(session, census_tract, ['statefp', 'countyfp', 'tractce'])

census_tract_geom = vapil.aggregate_geom_union(session=session, rmo=census_tract, agg_on_attr='geom', 
                                               agg_by_attr=['statefp', 'countyfp', 'tractce'], aggLabel='tract_geom')

census_tract_geom = vapi.materialize_rmo(session, census_tract_geom, name_prefix='census_tract_geom', reload=True)

session.commit()
[csv_fn, file_elem] = vapi.export_rmo(session, census_tract_geom, model_name='census_tract_geom', delimiter='|', 
                                      metadata_fn='census_tract_geom.md')
#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::


#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
#compute vertices in network relations
#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

vertices_geo = llg.add_point_geom_col(session, vertex_geo,  lon_attr='longitude', lat_attr='latitude', point_attr='coord')
vertices = vapi.materialize_rmo(session, vertices_geo, name_prefix='road_network_vertices',reload=True)
vertices_in_tract = vra.fuseCoversUnique(session, census_tract_geom, vertices,'tract_geom', 'coord', ['id'])
vertices_in_tract = sc.proj(session, vertices_in_tract, ['id', 'statefp', 'countyfp', 'tractce'])
vapi.export_rmo(session, vertices_in_tract, model_name='vertex_geo_w_tract', delimiter='|', metadata_fn='vertex_geo_w_tract.md')

#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
#locate vertices not in any tract
#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
in_tract_ids = vapi.proj(session, vertices_in_tract, ['id'])
all_ids = vapi.proj(session, vertex_geo, ['id'])
unaccounted_for = vapi.set_diff(session, all_ids, in_tract_ids)
with_coord = vapi.fuseEQ(session, vertex_geo, unaccounted_for, 'id')
unaccounted_for.c.keys()
unaccounted_for = vapi.set_diff(session, all_ids, in_tract_ids, preserve_column_name=True)
with_coord = vapi.fuseEQ(session, vertex_geo, unaccounted_for, 'id')
#res = vapi.scan(session, with_coord)


#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
#plot them
#:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
