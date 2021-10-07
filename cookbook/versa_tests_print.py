from header import *
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import mapper, sessionmaker, aliased
import versa_api as vapi
from versa_api_utils import print_table

engine = create_engine('sqlite:///:memory:', echo=False)
metadata = sa.MetaData()
tt = sa.Table("tt", metadata,
    sa.Column("aa", sa.Integer, primary_key=True),
    sa.Column("bb", sa.String(255), primary_key=True, nullable=False),
    sa.Column("cc", sa.Integer, primary_key=True, nullable=False),

    )

class oo(object):
    def __init__(self, aa, bb, cc):
        self.aa = aa
        self.bb = bb
        self.cc = cc
    def __repr__(self):
       return "<Device(%s, '%s', '%c')>" % (self.aa,
                                           self.bb, self.cc
                                      )

mapper(oo, tt)

metadata.create_all(engine)

db_session = sessionmaker(bind=engine)()

for avar in [1, 2]:
    for bvar in ['a', 'b']:
        for cvar in [9,8,7]:
            oi=oo(avar, bvar, cvar)
            db_session.add(oi)


res = vapi.scand(db_session, oo, 'aa')
assert(1 in res)

res = vapi.scan(db_session, oo)
#assert(len(res) == 64)


#res = vapi.scan(db_session, vapi.limit(db_session, oo, 2))
#assert(len(res) == 2)
print "limit"
print_table(db_session, vapi.limit(db_session, oo, 2))


print "random_select_k"
print_table(db_session, vapi.random_select_k(db_session, oo, 4))


print "proj"
print_table(db_session,vapi.proj(db_session, oo, ['aa', 'bb']))


print "distinct/proj"
print_table(db_session, vapi.distinct(db_session, vapi.proj(db_session, oo, ['aa', 'bb']), 'aa'))

print "drop"
print_table(db_session, vapi.drop(db_session, oo, 'bb'))

print "filterEQ"
print_table(db_session, vapi.filterEQ(db_session, oo, 'bb', 'a'))

# print "filterNEQ"
# print vapi.scan(db_session, vapi.filterNEQ(db_session, oo, 'bb', 'a'))
# print vapi.scan(db_session, vapi.filterGT(db_session, oo, 'bb', 'a'))
# print vapi.scan(db_session, vapi.filterGTE(db_session, oo, 'bb', 'b'))
# print vapi.scan(db_session, vapi.filterLT(db_session, oo, 'aa', 4))
# print vapi.scan(db_session, vapi.filterLTE(db_session, oo, 'aa', 3))
# print vapi.scan(db_session, vapi.filterLTE(db_session, oo, 'aa', 3))
# print vapi.scan(db_session, vapi.filterRangeClosed(db_session, oo, 'aa', 1, 3))
# print vapi.scan(db_session, vapi.filterRangeOpen(db_session, oo, 'aa', 1, 3))


# print vapi.scan(db_session,
#                 vapi.filterAttrEQ(db_session, oo, 'aa', 'cc')
#                 )

# print vapi.scan(db_session,
#                 vapi.filterAttrNEQ(db_session, oo, 'aa', 'cc')
#                 )
                
# print vapi.scan(db_session,
#                 vapi.ascending(db_session, 
#                                vapi.random_select_k(db_session, oo, 10),
#                                'bb')
#                 )

# print vapi.scan(db_session,
#                 vapi.filterAttrNEQ(db_session, oo, 'aa', 'cc')
#                 )
            
# print vapi.scan(db_session,
#                 vapi.aggregate(db_session, 
#                                vapi.random_select_k(db_session, oo, 40),
#                                'aa')
#                 )

# sample_stmt = vapi.random_select_k(db_session, oo, 64)
# print vapi.scan(db_session,
#                  vapi.aggregate(db_session, sample_stmt, 'aa')
#                 )
# print vapi.scan(db_session,
#                 vapi.cardinality_distribution(db_session, sample_stmt, 'aa')
#                 )


# sample_stmt1 = vapi.limit(db_session, oo, 3)
# sample_stmt2 = vapi.limit(db_session, oo, 3)
# print vapi.scan(db_session, vapi.set_diff(db_session, sample_stmt1, sample_stmt2))

# print vapi.cardinality(db_session, vapi.random_select_k(db_session, oo, 40))            


                
