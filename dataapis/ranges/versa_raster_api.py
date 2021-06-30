from versa_api_meta import buildstmt
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer
from geoalchemy2 import Geometry


#def equals(session=None, rmo=None, range_attr=None, arg_range=None):
#    '''return all tuples where range_attr equals arg_range'''
#    robject = buildstmt(session, rmo)
#    stmt = session.query(robject).filter(getattr(robject.c, range_attr)==arg_range).subquery()
#    return stmt

#stmt = vra.containedBy(session, Department, 'range', '[1, 500]')
#assert(len(vapi.scan(session, stmt)) == 3)

def getPointsInPolygon(session=None, rmo=None, raster_attr=None, polygon=None):
    '''raster points in a polygon'''
    robject = buildstmt(session, rmo)
    engine = session.connection().engine
    connection = engine.connect()
    connection.execute("""CREATE TABLE IF NOT EXISTS """+rmo.__tablename__+"""_raster AS SELECT gv.x, gv.y, gv.val, ST_AsText((gv).geom) geom FROM (SELECT (ST_PixelAsPolygons(rast)).* from """+rmo.__tablename__+""") gv;""")
    connection.close()
    Base = declarative_base()
    class raster(Base):
        """"""
        __tablename__ = rmo.__tablename__+"""_raster"""
        x = Column(Integer)
        y = Column(Integer)
        val = Column(Integer)
        geom = Column(Geometry('POLYGON'), primary_key=True)

    stmt = session.query(raster).filter(raster.geom.ST_Intersects(polygon)).subquery()
    return stmt

def getAllPolygonsInRaster(session=None, rmo=None, raster_attr=None):
    '''raster points in a polygon'''
    robject = buildstmt(session, rmo)
    engine = session.connection().engine
    connection = engine.connect()
    connection.execute("""CREATE TABLE IF NOT EXISTS """+rmo.__tablename__+"""_raster AS SELECT gv.x, gv.y, gv.val, ST_AsText((gv).geom) geom FROM (SELECT (ST_PixelAsPolygons(rast)).* from """+rmo.__tablename__+""") gv;""")
    connection.close()
    Base = declarative_base()
    class raster(Base):
        """"""
        __tablename__ = rmo.__tablename__+"""_raster"""
        x = Column(Integer)
        y = Column(Integer)
        val = Column(Integer)
        geom = Column(Geometry('POLYGON'), primary_key=True)

    stmt = session.query(raster).subquery()
    return stmt


