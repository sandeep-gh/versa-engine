from sqlalchemy import Table, Column, Integer, Unicode, MetaData, create_engine

from sqlalchemy import Table, Column, Integer, Unicode, MetaData, create_engine
from sqlalchemy.orm import mapper, create_session
import sqlalchemy as sa
from sqlalchemy import PrimaryKeyConstraint
import dataapis.utils as vu
import dataapis.export as ex
import dataapis.schema as sch
def build_ranges(session=None,  prefix=None,  start=0, end=None, step=None):
    '''
    create table with columns <prefix>_id, <prefix>_lb, <prefix>_ub
    '''
    return build_ranges_impl(session=session,  seq_label=prefix+"_id", range_lb=prefix+"_lb", range_ub=prefix+"_ub", start=start, step=step, end=end)


def build_ranges_impl(session=None,  seq_label=None, range_lb=None, range_ub=None, start=None, step=None, end=None):
    tbl_name = 't_' + seq_label
    cls_name =  seq_label
    
    if reload:
        if vu.check_model_exists(cls_name):
            print ("model already exists...skipping operation")
            return vu.import_model(cls_name)

    seq_orm = vapi.create_mapped_table(session, tbl_name='t_' + seq_label, cls_name=cls_name, attrs=[range_lb, range_ub], types=[Integer(), Integer()], primary_keys=[True, True], indexes=[True, True])

    for val in range(start, end, step):
        si = seq_orm()
        lb = val
        ub = val+step
        si.__dict__[range_lb] = lb
        si.__dict__[range_ub] = ub
        session.add(si)
    
    ex.write_rmo_def(session, seq_orm, cls_name=cls_name,  tbl_name=tbl_name)
    #session.commit()
    return seq_orm


def create_rmo_sequence(session=None,  rmo_label=None, sequence_attr=None, start=0, step=1, end=None):
    """create a table and the corresponding rmo with column sequence_attr with entries [start:end:step]
    """
    cls = sch.create_mapped_table(session, tbl_name='t_' + rmo_label, cls_name=rmo_label, attrs=[sequence_attr], types=[Integer()], primary_keys=[True], indexes=[True])
    for val in range(start, end, step):
        si = cls()
        si.__dict__[sequence_attr] = val
        session.add(si)
    rmo = session.query(cls).subquery()
    return rmo
