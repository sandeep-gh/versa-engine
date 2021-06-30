import inspect
import sqlalchemy
from sqlalchemy.orm import class_mapper, defer
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Column, Integer, String, Table
from  sqlalchemy.sql.expression import func, select, desc
import sys

db_session=None

def set_session(session):
    global db_session
    db_session=session



def getAttrList(object, cols):
    all_attrs = []
    for col in cols:
        all_attrs.append(getattr(object.c, col))
    return all_attrs



def getAttrList_but(object, cols):
    all_attrs = []
    for col in object.c.keys():
        if(not col in cols):
            all_attrs.append(col)
    return all_attrs

def getColNames(session, object):
    robject = object
    if(type(object) == sqlalchemy.ext.declarative.DeclarativeMeta):
        robject = session.query(object).subquery()
    return robject.columns.keys()



def buildstmt(session, object):
    if(type(object) == sqlalchemy.ext.declarative.DeclarativeMeta):
        return session.query(object).subquery()
    return object

def get_pkeys(session, object):
    cols = getColNames(session, object)
    pkeys = [col  for col in cols if getattr(object.c, col).primary_key]
    return pkeys

def set_primary_keys(session, object, cols):
    '''
    set all the cols  in object as primary key
    '''
    robject = object
    
    for col in cols:
        getattr(robject.c, col).primary_key = True
    return robject

def carry_over_primary_keys(session, ormo, drmo, attrl=None):
    '''ormo: original statement; drmo: derived statement
    '''
    if attrl is None:
        attrl = get_pkeys(session, ormo)

    for attr in attrl:
        if getattr(ormo.c, attr).primary_key:
            if attr in drmo.c:
                getattr(drmo.c, attr).primary_key = True
                
    return drmo





def rename_attribute(session=None, rmo=None, c_attr=None, c_label=None):
    '''
    assign a new name to the attribute
    '''

    robject = buildstmt(session, rmo)
    all_attrl = getColNames(session, robject)
    aliased_attrl = []
    for attr in all_attrl:
        if attr == c_attr:
            aliased_attrl.append(getattr(robject.c, attr).label(c_label))
        else:
            aliased_attrl.append(getattr(robject.c, attr))
            
    return session.query(*aliased_attrl).subquery()


    
def get_base_table(session=None, cls=None):
    """returns the table reference of a mapped class"""
    mapper = inspect(cls)
    return mapper.tables[0]


from datetime import datetime, date
from sqlalchemy.orm.query import Query

def explain_query(session,rmo):
    final_query = render_query(session,rmo)
    explain_raw=session.execute('EXPLAIN ANALYSE ' + final_query).fetchall()
    explain_formatted=[ line.values()[0] for line in explain_raw ]
    return '\n'.join(explain_formatted)

def render_query(session, rmo, bind=None):
    """
    Code from: https://gist.github.com/gsakkis/4572159
    Generate an SQL expression string with bound parameters rendered inline
    for the given SQLAlchemy statement.

    WARNING: This method of escaping is insecure, incomplete, and for debugging
    purposes only. Executing SQL statements with inline-rendered user values is
    extremely insecure.

    Based on http://stackoverflow.com/questions/5631078/sqlalchemy-print-the-actual-query
    """

    statement = session.query(rmo)
    
    if isinstance(statement, Query):
        if bind is None:
            bind = statement.session.get_bind(statement._mapper_zero())
        statement = statement.statement
    elif bind is None:
        bind = statement.bind

    class LiteralCompiler(bind.dialect.statement_compiler):

        def visit_bindparam(self, bindparam, within_columns_clause=False,
                            literal_binds=False, **kwargs):
            return self.render_literal_value(bindparam.value, bindparam.type)

        def render_literal_value(self, value, type_):
            if isinstance(value, long):
                return str(value)
            elif isinstance(value, (date, datetime)):
                return "'%s'" % value
            return super(LiteralCompiler, self).render_literal_value(value, type_)

    return LiteralCompiler(bind.dialect, statement).process(statement)

