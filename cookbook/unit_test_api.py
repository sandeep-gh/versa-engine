####  NEED TO FIRST RUN 
#normal VERSA startup
#update following line
####
dbsession = 'JohnTestJob132'
import runner
import range
import sys
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from range import IntRangeType
from intervals import IntInterval
from sqlalchemy.orm import sessionmaker

import versa_range_api as vra
import versa_api as vapi

session = build_session(dbsession)
set_session(session)

#Creates table
engine = session.connection().engine
connection = engine.connect()
connection.execute("""DROP TABLE IF EXISTS department;""")
connection.execute("""CREATE TABLE department(id INT PRIMARY KEY, name text, range NUMRANGE);""")
connection.close()

Base = declarative_base()

class Department(Base):
    __tablename__ = 'department'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    range = Column(IntRangeType(step=1000))

d = Department(id=1,name="IT1",range=[1,1000])
session.add(d)
d = Department(id=2, name="IT2",range=[1,500])
session.add(d)
d = Department(id=3, name="IT3",range=[501,1000])
session.add(d)
d = Department(id=4, name="IT4",range=[1,250])
session.add(d)
d = Department(id=5, name="IT5",range=[251,500])
session.add(d)
d = Department(id=6, name="IT6",range=[501,750])
session.add(d)
d = Department(id=7, name="IT7",range=[751,1000])
session.add(d)
session.commit()

print "testing contains"
stmt = vra.contains(session, Department, 'range', '[1, 500]')
assert(len(vapi.scan(session, stmt)) == 2)
stmt = vra.contains(session, Department, 'range', '[1, 501]')
assert(len(vapi.scan(session, stmt)) == 1)
stmt = vra.contains(session, Department, 'range', '[100, 100]')
assert(len(vapi.scan(session, stmt)) == 3)

print "testing findParents"
stmt = vra.findParents(session, Department, 'range', '[1, 1000]')
assert(len(vapi.scan(session, stmt)) == 1)
stmt = vra.findParents(session, Department, 'range', '[1, 500]')
assert(len(vapi.scan(session, stmt)) == 2)
stmt = vra.findParents(session, Department, 'range', '[1, 501]')
assert(len(vapi.scan(session, stmt)) == 1)
stmt = vra.findParents(session, Department, 'range', '[100, 100]')
assert(len(vapi.scan(session, stmt)) == 3)

print "testing containedBy"
stmt = vra.containedBy(session, Department, 'range', '[1, 500]')
assert(len(vapi.scan(session, stmt)) == 3)
stmt = vra.containedBy(session, Department, 'range', '[1, 501]')
assert(len(vapi.scan(session, stmt)) == 3)
stmt = vra.containedBy(session, Department, 'range', '[0, 500]')
assert(len(vapi.scan(session, stmt)) == 3)
stmt = vra.containedBy(session, Department, 'range', '[0, 501]')
assert(len(vapi.scan(session, stmt)) == 3)
stmt = vra.containedBy(session, Department, 'range', '[100, 100]')
assert(len(vapi.scan(session, stmt)) == 0)
stmt = vra.containedBy(session, Department, 'range', IntInterval('[1, 1000]'))
assert(len(vapi.scan(session, stmt)) == 7)

print "testing findChildren"
stmt = vra.findChildren(session, Department, 'range', '[1, 500]')
assert(len(vapi.scan(session, stmt)) == 3)
stmt = vra.findChildren(session, Department, 'range', '[1, 501]')
assert(len(vapi.scan(session, stmt)) == 3)
stmt = vra.findChildren(session, Department, 'range', '[0, 500]')
assert(len(vapi.scan(session, stmt)) == 3)
stmt = vra.findChildren(session, Department, 'range', '[0, 501]')
assert(len(vapi.scan(session, stmt)) == 3)
stmt = vra.findChildren(session, Department, 'range', '[100, 100]')
assert(len(vapi.scan(session, stmt)) == 0)
stmt = vra.findChildren(session, Department, 'range', IntInterval('[1, 1000]'))
assert(len(vapi.scan(session, stmt)) == 7)


print 'testing find between'
stmt = vra.findBetween(session, Department, 'range', '[1, 1000]', '[1, 250]')
assert(len(vapi.scan(session, stmt)) == 3)
stmt = vra.findBetween(session, Department, 'range', '[1, 1000]', '[1, 1000]')
assert(len(vapi.scan(session, stmt)) == 1)
stmt = vra.findBetween(session, Department, 'range', '[501, 1000]', '[1, 500]')
assert(len(vapi.scan(session, stmt)) == 0)


print "testing equals"
stmt = vra.equals(session, Department, 'range', IntInterval('[1, 1000]'))
assert(len(vapi.scan(session, stmt)) == 1)
stmt = vra.equals(session, Department, 'range', IntInterval('[1, 1001]'))
assert(len(vapi.scan(session, stmt)) == 0)
stmt = vra.equals(session, Department, 'range', IntInterval('[0, 1000]'))
assert(len(vapi.scan(session, stmt)) == 0)


print "testing length equals"
stmt = vra.lengthEquals(session, Department, 'range', '998')
assert(len(vapi.scan(session, stmt)) == 1)
stmt = vra.lengthEquals(session, Department, 'range', 248)
assert(len(vapi.scan(session, stmt)) == 4)
stmt = vra.lengthEquals(session, Department, 'range', 4)
assert(len(vapi.scan(session, stmt)) == 0)


print 'testing finding the root'
stmt = vra.findSharedParents(session, Department, 'range', '[1, 250]', '[251, 500]')
assert(len(vapi.scan(session, stmt)) == 2)
stmt = vra.findSharedParents(session, Department, 'range', '[1, 250]', '[501, 750]')
assert(len(vapi.scan(session, stmt)) == 1)
stmt = vra.findSharedParents(session, Department, 'range', '[251, 251]', '[700, 700]')
assert(len(vapi.scan(session, stmt)) == 1)
stmt = vra.findSharedParents(session, Department, 'range', '[1, 250]', '[1, 250]')
assert(len(vapi.scan(session, stmt)) == 3)
stmt = vra.findSharedParents(session, Department, 'range', '[1, 500]', '[1, 250]')
assert(len(vapi.scan(session, stmt)) == 2)

print 'testing finding most recent parent'
stmt = vra.findMostRecentParent(session, Department, 'range', '[1, 250]', '[251, 500]')
assert(len(vapi.scan(session, stmt)) == 1)
stmt = vra.findMostRecentParent(session, Department, 'range', '[1, 250]', '[251, 500]')
assert(vapi.scan(session, stmt)[0].id == 2)
stmt = vra.findMostRecentParent(session, Department, 'range', '[1, 250]', '[501, 750]')
assert(vapi.scan(session, stmt)[0].id == 1)
stmt = vra.findMostRecentParent(session, Department, 'range', '[251, 251]', '[700, 700]')
assert(vapi.scan(session, stmt)[0].id == 1)
stmt = vra.findMostRecentParent(session, Department, 'range', '[1, 250]', '[1, 250]')
assert(vapi.scan(session, stmt)[0].id == 4)

