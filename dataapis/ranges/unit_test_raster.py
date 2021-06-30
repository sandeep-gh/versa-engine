####  NEED TO FIRST RUN 
#normal VERSA startup
#update following line
####
dbsession = 'JohnTestJob55'
import runner
import range as rng
import sys
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from range import IntRangeType
from intervals import IntInterval
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Raster, Geometry
from geoalchemy2.elements import WKTElement, RasterElement
import versa_range_api as vra
import versa_api as vapi

session = build_session(dbsession)
set_session(session)

#Here open psql and run CREATE EXTENSION postgis;
#Creates example raster table
engine = session.connection().engine
connection = engine.connect()
connection.execute("""CREATE EXTENSION postgis;""")
connection.execute("""DROP TABLE IF EXISTS bio_15;""")
connection.execute("""CREATE TABLE bio_15(rid INT PRIMARY KEY, rast Raster);""")
connection.close()

Base = declarative_base()

class Bio_15(Base):
    __tablename__ = 'bio_15'
    rid = Column(Integer, primary_key=True)
    rast = Column(Raster)
    def __init__(self,rid, rast):
        self.rid = rid
        self.rast = rast

#Adds a raste rto the raster table
#Can instead run  raster2pgsql -I  -C /groups/NDSSL/EpistudyEbola/Bio_15.tif |psql -p 7622 postgres 
polygon = WKTElement('POLYGON((10 10,15 15,10 15,10 10))', srid=4326)
bioRaster = Bio_15(2, polygon.ST_AsRaster(5, 5))
session.add(bioRaster)
session.commit()

#Creates a polygon representation of the raster table
engine = session.connection().engine
connection = engine.connect()
#connection.execute("""SELECT gv.x, gv.y, gv.val, ST_AsText((gv).geom) geom FROM (SELECT (ST_PixelAsPolygons(rast)).* from bio_15) gv;""")
connection.execute("""CREATE TABLE IF NOT EXISTS  rasterpolygonrepresentation1 AS SELECT gv.x, gv.y, gv.val, ST_AsText((gv).geom) geom FROM (SELECT (ST_PixelAsPolygons(rast)).* from bio_15) gv;""")
connection.close()


class rasterRep1(Base):
    """"""
    __tablename__ = 'rasterpolygonrepresentation1'
    x = Column(Integer)
    y = Column(Integer)
    val = Column(Integer) 
    geom = Column(Geometry('POLYGON'), primary_key=True)


query = session.query(rasterRep1).filter(rasterRep1.geom.ST_Intersects('LINESTRING(10 15,13 12)'))
assert query.count == 7

query = session.query(rasterRep1)
assert query.count == 15





subq = query.subquery()
from shapely import wkbi
import versa_api as vapi

allXArray= []
allYArray= []
for line in  vapi.scan(session, subq):
    square = wkb.loads(bytes(line.geom.data))
    xArray,yArray = square.exterior.coords.xy
    allXArray.append(xArray)
    allYArray.append(yArray)

color = []
for i in range(len(allXArray)):
    if i%2==0:
      color.append("Red")
    else:
      color.append("Blue")

import mappingAPI as mapping
#the 0 and 30 will need to be changed to proper left, right, bottom, top coordinates
mapping.createFishnetMap(0, 30, 0, 30, resolution = 'l', saveLocation = "foo.png",xCoords=allXArray, yCoords=allYArray,colorList=color)





#Old stuff
#TODO delete this when everything works

#stmt = vra.contains(session, rasterRep1, 'x', 1)
#assert(len(vapi.scan(session, stmt)) == 2)


#assert isinstance(bioRaster.rast, RasterElement)
#height = session.execute(bioRaster.rast.ST_Height()).scalar()
#assert height == 5
#width = session.execute(bioRaster.rast.ST_Width()).scalar()
#assert width == 5

 # The top left corner is covered by the polygon
#top_left_point = WKTElement('Point(10 11)', srid=4326)
#top_left = session.execute(
#bioRaster.rast.ST_Value(top_left_point)).scalar()
#assert top_left == 1

#top_left_point = WKTElement('Point(10 15)', srid=4326)
#top_left = session.execute(
#bioRaster.rast.ST_Value(top_left_point)).scalar()
#assert top_left == 1

#top_left_point = WKTElement('Point(16 15)', srid=4326)
#3top_left = session.execute(
#bioRaster.rast.ST_Value(top_left_point)).scalar()
#assert top_left == None





#a = session.query(Bio_15)
#raster = a[0].rast
#rid = a[0].rid
# print raster.ST_Height()
# print raster.ST_Width()




