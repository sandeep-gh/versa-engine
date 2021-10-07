from header import *
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import mapper, sessionmaker, aliased
import versa_api as vapi
import random
from sqlalchemy.orm.util import class_mapper

#engine = create_engine('sqlite:///:memory:', echo=False)
metadata = sa.MetaData() 
nodes_tbl = sa.Table("nodes_tbl", metadata,
    sa.Column("pid", sa.Integer, primary_key=True), 
    )

edges_tbl = sa.Table("edges_tbl", metadata,
    sa.Column("sid", sa.Integer, primary_key=True),
    sa.Column("did", sa.Integer, primary_key=True, nullable=False),
    )


class node(object):
    def __init__(self, pid):
        self.pid = pid
    def __repr__(self):
       return "<node(%s)>" % (self.pid)

mapper(node, nodes_tbl)


class edge(object):
    def __init__(self,  sid, did):
        self.sid = sid
        self.did = did
    def __repr__(self):
       return "<edge(%s, %s)>" % (self.sid, self.did)

mapper(edge, edges_tbl)


engine = session.connection().engine
metadata.create_all(engine)

#db_session = sessionmaker(bind=engine)()
db_session = session
num_nodes = 100
for nid in range(num_nodes):
    n = node(nid)
    db_session.add(n)


p = 0.7
for avar in range(num_nodes):
    for bvar in range(num_nodes):
        if (random.random() < p):
            e = edge(avar, bvar)
            db_session.add(e)


# m_edge=class_mapper(edge)
# m_node = class_mapper(node)
# visited = Table('visited', metadata, Column('visited', Integer, primary_key=True))

db_session.commit()
