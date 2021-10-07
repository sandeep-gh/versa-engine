from sqlalchemy.sql.expression import cast
import sqlalchemy
from geoalchemy2.elements import WKTElement
import sqlalchemy as sa
import geoalchemy2
from geoalchemy2.types import Geometry

def add_point_geom_col(session, rmo, lat_attr, lon_attr, point_attr):
    """
    converts a pair latitute/longitute numbers to a point
    """
    rmo = vapi.buildstmt(session, rmo)
    lat_col = cast(getattr(rmo.c, lat_attr), sqlalchemy.String)
    lon_col = cast(getattr(rmo.c, lon_attr), sqlalchemy.String)
    stmt = session.query(rmo, sa.func.concat('Point(' + lon_col+' '+lat_col+')').label(point_attr+"_temp")).subquery()
    #point_col = cast(getattr(stmt.c, point_attr+"_temp").label(point_attr+'_string'), sqlalchemy.String)
    stmt = session.query(stmt, cast(getattr(stmt.c, point_attr+"_temp"), geoalchemy2.Geometry).label(point_attr)).subquery()
    #stmt = session.query(stmt, cast(getattr(stmt.c, point_attr+"_geom"), Geography).label(point_attr)).subquery()
#sa.func.concat(self.title, ' ', self.name)
#    stmt = session.query(rmo, cast(sa.func.concat('POINT(' + lat_col+''+lon_col+')'), WKTElement).label(point_attr))
    return stmt
