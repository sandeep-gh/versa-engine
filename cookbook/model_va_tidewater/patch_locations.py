from sqlalchemy import func
import sqlalchemy
from sqlalchemy.sql.expression import cast
from range import IntRangeType

import versa_geo as vg
import versa_fishnet_api as vfa
import lat_lon_to_geom as llg
import versa_api as vapi
import versa_api_list as vapil
import versa_impl as vi
import versa_range_api as vra



import sys

#import tidewater datasets
from persons_model import *
from household_model import *
from locations_model import * 

#quick  reference for the datasets
#persons --> (pid, hid)
#household --> (ID,  X,Y)
#locations (nonhome) --> (ID, longitude, latitude)


region = 'tidewater'
grid_sz = 1 #as in 1KM

#connect to database session
session = vi.init(sys.argv[1])


try:
    session.execute("create extension postgis")
    session.commit()
except:
    session.rollback()
    print "post gis already exists"



#relabel household(x,y) to household(longitude,latitude)
household_r = vapi.rename_attribute(session, 
                                    vapi.rename_attribute(session, household, 'x', 'longitude'),
                                    'y', 'latitude')

all_locations = vapi.set_union_rmo_pair(session, 
                                        rmoA=vapil.proj(session, household_r, ['id', 'longitude', 'latitude']),
                                        rmoB=vapil.proj(session, locations, ['location', 'longitude', 'latitude']),  preserve_column_name=True
                           )


# #define geometery rectangle  for the subregion

region_bb = vg.get_boundingbox(session=session, rmo=all_locations, lon_attr='longitude', lat_attr='latitude') #TODO: to go from dictionary to pyfunc arguments?

print region_bb
longitude_size = vfa.calc_distance(region_bb['ll_lat'], region_bb['ll_lon'], region_bb['ll_lat'], region_bb['ur_lon'])
latitude_size = vfa.calc_distance(region_bb['ll_lat'], region_bb['ll_lon'], region_bb['ur_lat'], region_bb['ll_lon'])

print "longitude_size = ", longitude_size
print "latitude_size = ", latitude_size

#create a geom object from the bounding box
region_bb_geom = vfa.bb_to_geom(ll_lat=region_bb['ll_lat'], ur_lat=region_bb['ur_lat'],ll_lon=region_bb['ll_lon'], ur_lon=region_bb['ur_lon']) 


#create a grid of grid_sz by grid_sz over the rectangle
print "creating fishnet table..."
fishnet = vfa.createFishNetTable(session=session, tableName=region+'_grid', clsName=region +'_grid', polygon=region_bb_geom, xSize=grid_sz, ySize=grid_sz) 
print "creating fishnet table...done"



#add coord attribute to locations
ll_c_f = vapil.proj(session, llg.add_point_geom_col(session, all_locations, lon_attr='longitude', lat_attr='latitude', point_attr='coord'),
                  ['id', 'coord']) 


#save the augmented table (for performance reasons...)
print "save locations with coord geom attribute..."
[t_ll_c, c_ll_c, ll_c] = vapi.materialize_rmo(session, ll_c_f, tbl_name='t_ll_c', cls='c_ll_c', indexes=['id', 'coord'],  cls_fn='ll_c.py', pks=['id'])
print "save locations with coord geom attribute...done"

#join locations with fishnet on location.coord 'within' fishnet.fishnet
fishnet_sub_ll_c = vfa.fuseContains(session, fishnet, ll_c, 'fishnet', 'coord')
#extract the fid-to-locationid relationship and materialize it
print "compute locations in grids..."
fishnet_sub_ll = vapil.proj(session, fishnet_sub_ll_c, ['fid', 'id'])
[t_sub_ll, c_sub_ll, sub_ll] = vapi.materialize_rmo(session, fishnet_sub_ll, tbl_name='t_sub_ll', cls='c_sub_ll', indexes=['id', 'fid'],  cls_fn='sub_ll.py', pks=['id'])
print "compute locations in grids...done"
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

session.commit()


