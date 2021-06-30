#placeholder to collect shapefile geom operators
#borrowing from fishnet api

from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func, Float
from math import cos, sin, asin, sqrt, radians
from geoalchemy2.elements import WKTElement
from shapely import wkb
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Raster, Geometry
import geoalchemy2.shape
from shapely.geometry import polygon as pg
from geoalchemy2.types import Geometry
from sqlalchemy.sql.expression import cast

import dataapis.ranges.lat_lon_to_geom as lltg
import versa_api_meta



#https://tomholderness.wordpress.com/tag/geoalchemy2/
def get_centroid_geom(session, rmoShape=None, geom_attr=None, res_label='centroid'):
    """
    Computes the center of mass of the geometry
    """
    rmoShape = versa_api_meta.buildstmt(session, rmoShape)
    stmt=session.query(rmoShape, cast((getattr(rmoShape.c, geom_attr).ST_Centroid()), Geometry("Point")).label(res_label)).subquery()    
    return stmt

def get_distance_AB(session, rmoShape=None, coordA=None, coordB=None, res_label=None):
    """  
    finds the 2-dimensional cartesian minimum distance between two geometries    
    """
    rmoShape = versa_api_meta.buildstmt(session, rmoShape)
    stmt=session.query(rmoShape, cast(func.ST_Distance(getattr(rmoShape.c, coordA), getattr(rmoShape.c, coordB)), Float).label(res_label)).subquery()    
    return stmt
