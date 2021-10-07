import runner
import range
import sys
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from range import IntRangeType
from intervals import IntInterval
from sqlalchemy.orm import sessionmaker
import versa_api_list as vapil
import versa_range_api as vra
import versa_api as vapi
from sqlalchemy.orm import mapper
import versa_impl as vi
from sqlalchemy.orm import composite

from Point import Point

session = vi.init("test13")

engine = session.connection().engine
connection = engine.connect()
connection.execute("""DROP TABLE IF EXISTS department;""")

connection.execute("""CREATE TABLE department(id INT PRIMARY KEY, id2 INT, id3 INT,name text, range NUMRANGE);""")
#connection.execute("""CREATE TABLE department(id INT PRIMARY KEY, id2 INT, id3 INT,name text, range NUMRANGE, start Point);""")#unneeded statement(That will probably cause issues)
connection.close()


Base = declarative_base()

class Department(Base):
        __tablename__ = 'department'
        id = Column(Integer, primary_key=True)
        id2 = Column(Integer)
        id3 = Column(Integer)
        name = Column(String)
        range = Column(IntRangeType(step=1000))
        start = composite(Point, id2, id3)


d = Department(id=1,id2=1, id3=1,name="IT1",range=[1,1000])
session.add(d)
d = Department(id=2,id2=2, id3=2,name="IT1",range=[1,1000])
session.add(d)
d = Department(id=3, id2=3, id3=3,name="IT1",range=[1,1000])
session.add(d)
d = Department(id=4, id2=3, id3=3,name="IT1",range=[1,1000])
session.add(d)
d = Department(id=5, id2=3, id3=3,name="IT1",range=[1,1000])
session.add(d)

base = vra.containedBy(session, Department, 'range', IntInterval('[1, 1000]'))
vapi.scan(session, base)

stmt = session.query(Department).filter(Department.start == Point(3, 3))    #works
vapi.scan(session, stmt.subquery())

a=vapi.filterEQ(session, rmo=Department, attr="start", value=Point(3,3))   #broken statement
vapi.scan(session, a)

aggregate = vapil.aggregate_array(session=session, rmo=Department, agg_key=['id2'], arr_attrl= ['id'])  #doesn't work
