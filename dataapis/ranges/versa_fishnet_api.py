###########EXAMPLE RUNNABLE CODE###########################
#from shapely.geometry import polygon as pg
#import geoalchemy2.shape
#import versa_fishnet_api as vf
#import versa_api_utils


#pointlist=[[0,0],[10,0],[10,10],[0,10]]
#a=pg.Polygon(pointlist)
#wkb_element = geoalchemy2.shape.from_shape(a)
#dbsession = dbsession = 'JohnTestJob8'
#session = versa_api_utils.build_session(dbsession)
#set_session(session)
#a=vf.createFishNetTable(session, "tableasdsadname29",  wkb_element, 50, ySize=50)

import versa_api_meta
import versa_utils as vu
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func, Float
from math import cos, sin, asin, sqrt, radians
from geoalchemy2.elements import WKTElement
from shapely import wkb
import versa_api as vapi
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Raster, Geometry
import geoalchemy2.shape
import lat_lon_to_geom as lltg
from shapely.geometry import polygon as pg
# add_point_geom_col(session, rmo, lat_attr, lon_attr, point_attr):

def fuseContains(session=None, rmoShape = None, rmoPoint = None, shape_attr = None, coord_attr=None):
    '''
    rmoPoint.coord_attr are Points (or other small shapes), and rmoShape.shape_attr are larger shapes.
    joins rmoPoint with rmoShape where
    rmoPoint.coord_attr is inside rmoShape.shape_attr (including edges)
    '''

    shapeObject = versa_api_meta.buildstmt(session, rmoShape)
    pointObject = versa_api_meta.buildstmt(session, rmoPoint)
    stmt=session.query(shapeObject, pointObject).filter(getattr(shapeObject.c, shape_attr).ST_Contains(getattr(pointObject.c, coord_attr))).subquery()    
    return stmt

def filterContains(session=None, rmoCoord = None, coord_attr = None, bounding_geom = None):
    """
    filters rmo based on condition coord_attr is contained in bounding_geom
    
    Args:

    Returns:
      A rmo that represent rows from rmo where rmoCoord.coord_attr is contained in bounding_geom
    """

    coordObject = versa_api_meta.buildstmt(session, rmoCoord)
    stmt=session.query(coordObject).filter(bounding_geom.ST_Contains(getattr(coordObject.c, coord_attr))).subquery()    
    return stmt
                                           

# def fuseContains(session=None, rmoShape = None, rmoPoint = None, shape = None, latitude = None, longitude = None):
# #def fuseEQ(session=None, rmoA=None, rmoB=None, attrA=None, attrB=None, preserve_columns=False):
#     shapeObject = versa_api_meta.buildstmt(session, rmoShape)
#     latLonObject = versa_api_meta.buildstmt(session, rmoPoint)
#     pointObject = lltg.add_point_geom_col(session, latLonObject, latitude, longitude, "point")
#     stmt=session.query(shapeObject, pointObject, getattr(shapeObject.c, shape), getattr(pointObject.c, "point")).filter(getattr(shapeObject.c, shape).ST_Contains(getattr(pointObject.c, "point"))).subquery()    
#     return stmt


def createFishNetTable(session=None, tableName=None, clsName=None, polygon=None, xSize=None, ySize=None):
    """
    Creates a fishnet of a polygon of size xSize and ySize (in meters).  
    The created table is named tableName, and class is clsName.  
    additionalCoordList also returns the x and Y list for python use. 
    """
    if vu.check_model_exists(clsName):
        c_fishnet = vu.import_model(clsName)
        return c_fishnet
    c_fishnet = vapi.create_mapped_table(session=session, tbl_name=tableName, cls_name=clsName, attrs=["fid","fishnet"], types=[Integer, Geometry('POLYGON')], primary_keys=[True,False], indexes=[False,False], is_temporary=False)
    #[t_fishnet, c_fishnet, fishnet_rmo] = vapi.create_table_and_rmo(session=session, tbl=tableName, cls=clsName, attrs=["fid","fishnet"], types=[Integer, Geometry('POLYGON')], primary_keys=[True,False], indexes=[False,False], is_temporary=False)
    polygons = _createFishNet(polygon,xSize,ySize)
    print("num polygons = ", len(polygons))
    all_polygons = []
    for i, polygon in enumerate(polygons):
        fishnet = c_fishnet()
        fishnet.fid = i
        fishnet.fishnet = polygon
        all_polygons.append(fishnet)
        #session.add(fishnet)

    print("number of grids in the fishnet = ", len(polygons))
    session.add_all(all_polygons)
    session.commit()
    
    vapi.write_rmo_def(session, c_fishnet, clsName, tableName)
    xList, yList = getXYCoordinates(polygon,xSize,ySize)
    return [fishnet, xList, yList]



def _getXYCoordinates(polygon,xSize=5,ySize=5):
    """
    helper for createFishNetTable when additionalCoordList==True
    creates a list of all x and y position used.
    (Looking at it I am not sure how or why this is useful, since it doesn't preserve x,y functionality)
    """

    ll_lat, ur_lat, ll_lon, ur_lon = unpackGeometry(polygon)
    xCoords = set()
    yCoords = set()
    currentLat = ll_lat
    currentLon = ll_lon
    while currentLat<ur_lat:
        while currentLon < ur_lon:
            nextLat, nextLon= _addKmToDegrees(currentLat, currentLon,xSize, ySize)
            xCoords.add(currentLat)
            xCoords.add(nextLon)
            xCoords.add(currentLon)
            yCoords.add(nextLat)
            yCoords.add(currentLat)
            currentLon = nextLon
        currentLon =ll_lon
        currentLat = nextLat
        print(currentLat, currentLon,xSize, ySize)
    return xCoords, yCoords



#Creates a list of polygons as a fishnet
def _createFishNet(polygon,xSize=5,ySize=5):
    """
    Creates a list of polygons as a fishnet    
    """
    ll_lat, ur_lat, ll_lon, ur_lon = unpackGeometry(polygon)
    polygonList = []
    currentLat = ll_lat
    currentLon = ll_lon
    while currentLat<ur_lat:
        while currentLon < ur_lon:
            nextLat, nextLon= _addKmToDegrees(currentLat, currentLon,xSize, ySize)
            polygonList.append(bb_to_geom(currentLat, nextLat, currentLon, nextLon))
            currentLon = nextLon
        currentLon =ll_lon
        currentLat = nextLat
    return polygonList

#Calculates the km distance between 2 points (in degrees)
def _calc_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    km = 6378 * c
    return km

def _addKmToDegrees(lat, lon, latChangeKm, lonChangeKm):
    """
    lat and lon are in degrees, latChangeK and lonChangeKm are in km
    Adds the km to the degrees, returning in degrees
    """
    dLatKM = calc_distance(lat, lon, lat+1, lon)
    dLonKM = calc_distance(lat, lon, lat, lon+1)
    dLatDegree = latChangeKm/dLatKM
    dLonDegree = lonChangeKm/dLonKM 
    return lat+dLatDegree, lon+dLonDegree

def bb_to_geom(ll_lat=None, ur_lat=None, ll_lon=None, ur_lon=None):
  """
  Turns coordinates into an object that can be added to a db
  """
  pointlist=[[ll_lon,ur_lat],[ur_lon,ur_lat],[ur_lon,ll_lat],[ll_lon,ll_lat]]
  a=pg.Polygon(pointlist)
  wkb_element = geoalchemy2.shape.from_shape(a)
  return wkb_element
#Unused
#def bb_to_geom_dict(dictionary):
  #return  WKTElement(
#  WKTElement('POLYGON(('+
#            str(dictionary['ll_lon'])+' '+str(dictionary['ll_lat'])+','+
#            str(dictionary['ll_lon'])+' '+str(dictionary['ur_lat'])+','+
#            str(dictionary['ur_lon'])+' '+str(dictionary['ur_lat'])+','+
#            str(dictionary['ur_lon'])+' '+str(dictionary['ll_lat'])+','+
#            str(dictionary['ll_lon'])+' '+str(dictionary['ll_lat'])+'))')

#Unpacks a WKBElement 
#Return ll_lat ur_lat ll_lon ur_lon

def unpackGeometry(polygon):
    """
    gets the bounding box coordinates from a polygon
    returns minX,maxX,minY,maxY
    """
    latitude = []
    longitude = []
    shapelyPolygon = wkb.loads(bytes(polygon.data))
    latitude, longitude = shapelyPolygon.exterior.coords.xy
    return min(latitude), max(latitude), min(longitude), max(longitude)

