import runner
import range
import sys
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from range import IntRangeType
from intervals import IntInterval
from sqlalchemy.orm import sessionmaker

import versa_impl as vi
import versa_range_api as vra
import versa_api as vapi


session=vi.init(sys.argv[1])

#Creates table
engine = session.connection().engine
connection = engine.connect()
connection.execute("""DROP TABLE IF EXISTS department;""")
connection.execute("""CREATE TABLE department(id INT PRIMARY KEY, name text, range INT);""")
connection.close()

Base = declarative_base()

class Department(Base):
    __tablename__ = 'department'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    range = Column(Integer)

d = Department(id=1,name="IT1",range=1000)
session.add(d)
d = Department(id=2, name="IT2",range=1000)
session.add(d)
d = Department(id=3, name="IT3",range=1000)
session.add(d)
d = Department(id=4, name="IT4",range=250)
session.add(d)
d = Department(id=5, name="IT5",range=500)
session.add(d)
d = Department(id=6, name="IT6",range=500)
session.add(d)
d = Department(id=7, name="IT7",range=1000)
session.add(d)
session.commit()


test1=vapi.over(session=session, rmo=Department,order_attr='id',partition_attr='range',attr_label="new_label")
assert vapi.scan(session,test1) == [(1L, 4), (1L, 5), (2L, 6), (1L, 1), (2L, 2), (3L, 3), (4L, 7)]
assert test1.c.has_key("new_label")

