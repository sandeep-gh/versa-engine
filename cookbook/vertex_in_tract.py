#readme: test geo points in tract polygon
import sys
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


session_name = sys.argv[1]
session = vapiu.build_session(session_name)
import time
st = time.time()
[census_tract_geom, vertex_geo] = [vu.import_model(model_name) for model_name in ['census_tract_geom', 'vertex_geo']]
print "total geo points = ", vapi.cardinality(session, vertex_geo)


vertices = llg.add_point_geom_col(session, vertex_geo, lat_attr='longitude', lon_attr='latitude', point_attr='coord')
vertices = vapi.materialize_rmo(session, vertices, tbl_name='t_vertices', cls='vertices', cls_fn='vertices_model.py', indexes=['coord'], pks=['id'], reload=True)

vertices_in_tract = vra.fuseCoversUnique(session, census_tract_geom, vertices,'tract_geom', 'coord', 'id')
#vapi.set_primary_keys(session, vertices, ['id'])
#vapi.export_rmo(session, vertices, model_name='vertex_geo_w_tract', delimiter='|', metadata_fn='vertex_geo_w_tract.md')
print "vertices in tract = ", vapi.cardinality(session, vertices_in_tract)
et = time.time()
print et-st
