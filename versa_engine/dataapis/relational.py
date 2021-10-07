import random
from sqlalchemy import MetaData, Column, Integer, String, Table, Index, Interval, Float, VARCHAR
from  sqlalchemy.sql.expression import func, select, desc, over, cast, literal_column
from sqlalchemy import Column, Integer, String, Table, Index, Interval, Float

import rmo.versa_api_meta as vam
from  rmo.versa_api_meta import buildstmt
from  rmo.versa_api_meta import rename_attribute
from rmo.versa_api_meta import getAttrList
import dataapis.schema as sch

from dataapis.schema import cast_integer


def select(session=None, cls=None):
    """Convert to subquery/rmo object
    
    Args:
        cls: a sqlachemcy declarative base class
        
    Returns:
        A subquery (or rmo) object. 
    """
    stmt=session.query(cls).subquery()
    return stmt



def limit(session=None, rmo=None, num_recs=None):
    """Restrict the  number of records
    
    Args:
        num_recs: number of records to be returned
        
    Returns:
        A new rmo with only num_recs from the input rmo

    TODO:
        We shouldn't be needing the carry_over_primary_keys
    """
    robject = vam.buildstmt(session, rmo)
    stmt = session.query(robject).limit(num_recs).subquery()
    stmt = vam.carry_over_primary_keys(session, robject, stmt)
    return stmt



def order_by_random(session=None, rmo=None):
    """Do a random permutation of the rows

    Args:


    Returns:
       A random permutation of the rows

    Caution:
      Can cause severe performance bottleneck if the rmo has large number of records
    """
    return session.query(rmo).order_by(func.random()).subquery()


def pick_at_random(session=None, rmo=None, num_rows=None):
    '''
    select a random row
    
    Args: 
       num_rows: number of rows in rmo (optional)

    '''
    if  num_rows is None:
        num_rows =  cardinality(session, rmo)
    return session.query(rmo).offset(random.randint(0,num_rows)).limit(1).subquery()

def pick_k_at_random(session=None, rmo=None, num_rows=None, k = None):
    '''
    select a random row
    
    Args: 
       num_rows: number of rows in rmo (optional)

    '''
    if  num_rows is None:
        num_rows =  cardinality(session, rmo)

    assert num_rows > k
    return session.query(rmo).offset(random.randint(0,num_rows-k)).limit(k).subquery()


def random_select_fraction(session=None, rmo=None, frac=0.1):
    '''
    select a random row
    
    Args: 
       num_rows: number of rows in rmo (optional)

    '''
    assert frac is not None
    num_rows  = cardinality(session, rmo)
    k =  int(frac * num_rows)
    return pick_k_at_random(session, rmo, num_rows, k)
    #stmt = limit(session, order_by_random(session, rmo), num_rows)
    #return stmt
     




def random_select_k(session=None, rmo=None, num_recs=None):
    '''
    select a random element 
    '''
    return limit(session, order_by_random(session, rmo), num_recs)




def is_empty(session=None, rmo=None):
    '''
    returns True is the table is empty
    '''
    flag = session.query(rmo).first()
    if flag is None:
        return True
    else:
        return False


def distinct(session=None, rmo=None, attrl=None):
    """
    returns distinct rows by attrl
    
    Args:
    attrl: list of attributes

    Returns:
    A rmo with attributes in attrl containing only distinct rows
    """
    robject = buildstmt(session, rmo)
    stmt=session.query(robject).distinct(*getAttrList(robject,attrl)).subquery()
    return stmt

# def distinct(session=None, rmo=None, attr=None):
#     robject = rmo
#     if(type(robject) == sqlalchemy.ext.declarative.DeclarativeMeta):
#         robject = select(session, rmo)
#     stmt=session.query(robject).distinct(getattr(robject.c, attr)).subquery()
#     return stmt

def filterEQ(session=None, rmo=None, attr=None, value=None):
    """
    filters rmo based on condition attr==value
    
    Args:

    Returns:
      A rmo that represent rows from rmo where rmo.attr == value
    """
    robject = buildstmt(session, rmo)
    stmt=session.query(robject).filter(getattr(robject.c, attr)==value).subquery()
    return stmt


def filterNEQ(session=None, rmo=None, attr=None, value=None):
    """
    filters rmo based on condition attr!=value
    
    Args:

    Returns:
      A rmo that represent rows from rmo where rmo.attr != value
    """
    robject = buildstmt(session, rmo)
    stmt=session.query(robject).filter(getattr(robject.c, attr)!=value).subquery()
    return stmt


def filterGT(session=None, rmo=None, attr=None, value=None):
    """
    filters rmo based on condition attr > value
    
    Args:

    Returns:
      A rmo that represent rows from rmo where rmo.attr != value
    """
    """
    filters rmo based on condition attr>value
    """
    robject = buildstmt(session,rmo)
    stmt=session.query(robject).filter(getattr(robject.c, attr)>value).subquery()
    return stmt


# def filterInterval(session=None, rmo=None, attr=None, value=None):
    
#     robject = buildstmt(session, rmo)
#     stmt=session.query(robject).filter(getattr(robject.c, attr)==value).subquery()
#     return stmt



def filterGTE(session=None, rmo=None, attr=None, value=None):
    """
    filters rmo based on condition attr >= value
    
    Args:

    Returns:
      A rmo that represent rows from rmo where rmo.attr >= value
    """

    robject = buildstmt(session,rmo)
    stmt=session.query(robject).filter(getattr(robject.c, attr)>=value).subquery()
    return stmt



def filterLT(session=None, rmo=None, attr=None, value=None):
    """
    filters rmo based on condition attr < value
    
    Args:

    Returns:
      A rmo that represent rows from rmo where rmo.attr < value
    """

    robject = buildstmt(session, rmo)
    stmt=session.query(robject).filter(getattr(robject.c, attr)<value).subquery()
    return stmt


def filterLTE(session=None, rmo=None, attr=None, value=None):
    """
    filters rmo based on condition attr >= value
    
    Args:

    Returns:
      A rmo that represent rows from rmo where rmo.attr <= value
    """

    robject = buildstmt(session, rmo)
    stmt=session.query(robject).filter(getattr(robject.c, attr)<=value).subquery()
    return stmt

def filterRangeClosed(session=None, rmo=None, attr=None, lval=None, rval=None):
    """
    filters rmo based on condition attr >= lval and attr <= rval
    
    Args:
      lval, rval: for the range [lval, rval] 

    Returns:
      A rmo that represent rows from rmo where rmo.attr in [lval, rval]

    """
    robject = buildstmt(session, rmo)
    stmt = filterGTE(session, robject, attr, lval)
    stmt = filterLTE(session, stmt, attr, rval)
    return stmt

def filterRangeOpen(session=None, rmo=None, attr=None, lval=None, rval=None):
    """
    filters rmo based on condition attr >= lval and attr < rval
    
    Args:
      lval, rval: for the range [lval, rval)

    Returns:
      A rmo that represent rows from rmo where rmo.attr in [lval, rval)

    """

    robject = buildstmt(session, rmo)
    stmt = filterGTE(session, robject, attr, lval)
    stmt = filterLT(session, stmt, attr, rval)
    return stmt

def ascending(session=None, rmo=None, attr=None):
    """
       orders rmo records based on attr in ascending order
    
    Args:
       attr: 

    Returns:
      A rmo that returns records in the ascending order of attr

    """

    robject = buildstmt(session, rmo)
    stmt=session.query(robject).order_by(getattr(robject.c, attr)).subquery()
    return stmt


def descending(session=None, rmo=None, attr=None):
    """
       orders rmo records based on attr in descending order
    
    Args:
       attr: 

    Returns:
      A rmo that returns records in the ascending order of attr

    """


    robject = buildstmt(session, rmo)
    stmt=session.query(robject).order_by(desc(getattr(robject.c, attr))).subquery()
    return stmt


def filter_attrEQ(session=None, rmo=None, attr1=None, attr2=None):
    robject = buildstmt(session, rmo)
    stmt=session.query(robject).filter(getattr(robject.c, attr1)== getattr(robject.c, attr2)).subquery()
    return stmt

def filter_attrNEQ(session=None, rmo=None, attr1=None, attr2=None):
    robject = buildstmt(session, rmo)
    stmt=session.query(robject).filter(getattr(robject.c, attr1)!= getattr(robject.c, attr2)).subquery()
    return stmt


def filter_attrLTE(session=None, rmo=None, attr1=None, attr2=None):
    robject = buildstmt(session, rmo)
    stmt=session.query(robject).filter(getattr(robject.c, attr1)<= getattr(robject.c, attr2)).subquery()
    return stmt


def min_attrs(session=None, rmo=None, attr1=None, attr2=None, min_attr=None):
    robject = buildstmt(session, rmo)
    stmt=session.query(robject, (func.least(getattr(robject.c, attr1), getattr(robject.c, attr2)).label(min_attr))).subquery()
    return stmt
    
def sum_attrs(session=None, rmo=None, attr1=None, attr2=None, sum_attr=None):
    robject = buildstmt(session, rmo)
    stmt=session.query(robject, (getattr(robject.c, attr1)+ getattr(robject.c, attr2)).label(sum_attr)).subquery()
    return stmt


def sub_attrs(session=None, rmo=None, attr1=None, attr2=None, sub_attr=None):
    robject = buildstmt(session, rmo)
    stmt=session.query(robject, (getattr(robject.c, attr1) -  getattr(robject.c, attr2)).label(sub_attr)).subquery()
    return stmt


def conc_attrs(session=None, rmo=None, attr1=None, attr2=None, conc_attr=None):
    robject = buildstmt(session, rmo)
    #stmt=session.query(robject, func.concat(getattr(robject.c, attr1), "/", getattr(robject.c, attr2)).label(conc_attr)).subquery()
    stmt=session.query(robject, func.concat(getattr(robject.c, attr1), getattr(robject.c, attr2)).label(conc_attr)).subquery()
    return stmt



def div_attrs(session=None, rmo=None, attr1=None, attr2=None, res_attr_label=None):
    robject = buildstmt(session, rmo)
    stmt=session.query(robject, (cast(getattr(robject.c, attr1), Float)/cast(getattr(robject.c, attr2), Float)).label(res_attr_label)).subquery()
    return stmt



def mult_attrs(session=None, rmo=None, attr1=None, attr2=None, mult_attr=None):
    robject = buildstmt(session, rmo)
    stmt=session.query(robject, (getattr(robject.c, attr1) *  getattr(robject.c, attr2)).label(mult_attr)).subquery()
    return stmt

def add_float_const_column(session=None, rmo=None, val=None, label=None):
    '''add a constant attribute with label and val
       currently only literal columns are supported
       TODO: if val is string starting with  002; then for some reason it resolves to 2
    '''
    robject = buildstmt(session, rmo)
    stmt = session.query(robject, literal_column(val, Float()).label(label)).subquery()
    return stmt




def build_array(session=None, rmo=None, attrl=None, res_attr_prefix=None):
    '''
    '''
    robject = buildstmt(session, rmo)
    if res_attr_prefix is None:
        res_attr_prefix = 'array_'
    if attrl is None:
        attrl = robject.c.keys()
    stmt = session.query(robject, literal_column("1").label('one')).subquery()
    arr_clause = []
    for attr in attrl:
        arr_clause.append(func.array_agg(getattr(stmt.c, attr)).label(res_attr_prefix+attr))
    stmt = session.query(*arr_clause).group_by(getattr(stmt.c, 'one')).subquery()
    return stmt

def aggregate_countall(session=None, rmo=None, res_attr_label=None):
    if res_attr_label is None:
        res_attr_label='total_count'
    robject = buildstmt(session, rmo)
    stmt= session.query(func.count(rmo).label(res_attr_label)).subquery()
    return stmt

def aggregate_array(session=None, rmo=None, group_by_attr=None, array_on_attr=None, res_attr_label = None):
    if res_attr_label is None:
        res_attr_label = 'array_'+ array_on_attr
    robject = buildstmt(session, rmo)
    stmt = session.query(getattr(robject.c, group_by_attr), func.array_agg(getattr(robject.c, array_on_attr)).label(res_attr_label)).group_by(getattr(robject.c, group_by_attr)).subquery()
    
    return stmt

def aggregate_sum(session=None, rmo=None, group_by_attr=None, sum_on_attr=None, res_attr_label=None):
    '''sum(values in sum_on_attr) per distinct values in group_by_attr; the summed column is labeled as sum_<sum_on_attr>'''
    if res_attr_label is None:
        res_attr_label = 'sum_'+sum_on_attr
    robject = buildstmt(session, rmo)
    stmt = session.query(getattr(robject.c, group_by_attr), func.sum(getattr(robject.c, sum_on_attr)).label(res_attr_label)).group_by(getattr(robject.c, group_by_attr)).subquery()
    return stmt

def aggregate_max(session=None, rmo=None, group_by_attr=None, max_on_attr=None, res_attr_label=None):
    '''max(values in sum_on_attr) per distinct values in sum_by_attr; the summed column is labeled as sum_<sum_on_attr>'''
    if res_attr_label is None:
        res_attr_label = 'max_'+max_on_attr
    robject = buildstmt(session, rmo)
    stmt = session.query(getattr(robject.c, group_by_attr), func.max(getattr(robject.c, max_on_attr)).label(res_attr_label)).group_by(getattr(robject.c, max_by_attr)).subquery()
    return stmt

def aggregate_min(session=None, rmo=None, group_by_attr=None, min_on_attr=None, res_attr_label=None):
    '''min(values in sum_on_attr) per distinct values in sum_by_attr; the summed column is labeled as sum_<sum_on_attr>'''
    if res_attr_label is None:
        res_attr_label = 'min_'+min_on_attr
    robject = buildstmt(session, rmo)
    stmt = session.query(getattr(robject.c, group_by_attr), func.min(getattr(robject.c, min_on_attr)).label(res_attr_label) ).group_by(getattr(robject.c, group_by_attr)).subquery()
    return stmt

def aggregate_avg(session=None, rmo=None, group_by_attr=None, avg_on_attr=None, res_attr_label=None):
    '''avg(values in sum_on_attr) per distinct values in sum_by_attr; the summed column is labeled as sum_<sum_on_attr>'''
    if res_attr_label is None:
        res_attr_label = 'avg_'+min_on_attr

    robject = buildstmt(session, rmo)
    if group_by_attr is not None:
        stmt = session.query(getattr(robject.c, group_by_attr), func.avg(getattr(robject.c, avg_on_attr)).label(res_attr_label)).group_by(getattr(robject.c, group_by_attr)).subquery()
    else:
        stmt = session.query(func.avg(getattr(robject.c, avg_on_attr)).label(res_attr_label)).subquery()
    return stmt

def aggregate_count(session=None, rmo=None, group_by_attr=None, res_attr_label=None):
    '''count(records) per distinct values in group_by_attr; the resulting column is labeled as count_<group_by_attr> if res_attr_label is not specified.

    '''
    if res_attr_label is None:
        res_attr_label = 'count'
    robject = buildstmt(session, rmo)
    #stmt = session.query(getattr(robject.c, group_by_attr), func.count(getattr(robject.c, group_by_attr)).label(res_attr_label)).group_by(getattr(robject.c, group_by_attr)).subquery()

    stmt = session.query(func.count().label(res_attr_label),
                         *getAttrList(robject, group_by_attr)).group_by(*getAttrList(robject, group_by_attr)).subquery()
    vam.set_primary_keys(session, stmt, group_by_attr)
    
    return stmt

def cardinality_distribution(session=None, rmo=None, container_attr=None, member_attr=None,  member_count_label='count', member_count_freq_label='freq'):
    '''rank-frequency distribution of occurance of values of attr, i.e., frequency distribution of the count per distinct value of attr'''
    robject = buildstmt(session, rmo)
    if member_attr is not None:
        robject = distinct(session, proj(session, rmo, [container_attr, member_attr]), [container_attr, member_attr])
    counts = aggregate(session, robject, container_attr, member_count_label)
    freq_of_counts = ascending(session, aggregate(session, counts, member_count_label, member_count_freq_label), member_count_label) 
    return freq_of_counts

def set_diff(session=None, from_rmo=None, without_rmo=None, preserve_column_name=False):
    '''output: set(from_rmo) - set(without_rmo), the columns names of the resulting table is the  same as the from_rmo if preserve_column_name is set to True
       required: both the tables, from_rmo and without_rmo, has to have the same schema'''
    stmt = session.query(from_rmo).except_(session.query(without_rmo)).subquery()
    if preserve_column_name is True:
        relabel_stmts = []
        original_col_names = from_rmo.c.keys()
        anon_col_names = stmt.c.keys()
        for on, an in zip(original_col_names, anon_col_names):
            assert(on in an)
            relabel_stmts.append(getattr(stmt.c, an).label(on))
        return session.query(*relabel_stmts).subquery()

    return stmt

def set_union_rmo_pair(session=None, rmoA=None, rmoB=None, keep_duplicates=False, preserve_column_name=False):
    '''output: set(from_rmo) union set(without_rmo), the columns names of the resulting table is the  same as the from_rmo if preserve_column_name is set to True, if keep_duplicates is True then records duplicated in union are kept, otherwise only distinct records are maintained
       required: both the tables, from_rmo and without_rmo, has to have the same schema'''
    if rmoA is None and rmoB is None:
        return None
    if rmoA is None:
        return rmoB
    if rmoB is None:
        return rmoA
    
    rmoA = buildstmt(session, rmoA)
    rmoB = buildstmt(session, rmoB)

    if keep_duplicates is False:
        stmt = (session.query(rmoA).union(session.query(rmoB))).subquery()
    else:
        stmt = (session.query(rmoA).union_all(session.query(rmoB))).subquery()
    if preserve_column_name is True:
        relabel_stmts = []
        original_col_names = rmoA.c.keys()
        anon_col_names = stmt.c.keys()
        for on, an in zip(original_col_names, anon_col_names):
            assert(on in an)
            relabel_stmts.append(getattr(stmt.c, an).label(on))
        return session.query(*relabel_stmts).subquery()
    return stmt

def set_union_recursive_impl(session=None, all_rmo=None, keep_duplicates=False, preserve_column_name=False):
    '''The flat impl of set union seems to be buggy
    '''
    if len(all_rmo) == 1:
        return all_rmo[0]
    rmoA = all_rmo[0]
    rmoB = all_rmo[1]
    stmt = set_union_rmo_pair(session, rmoA, rmoB, keep_duplicates=keep_duplicates, preserve_column_name=preserve_column_name)
    for rmo in all_rmo[2:]:
        stmt = set_union_rmo_pair(session, stmt, rmo, keep_duplicates=keep_duplicates, preserve_column_name=preserve_column_name)
    return stmt


def set_union(session=None, all_rmo=None, keep_duplicates=False, preserve_column_name=False):
    if len(all_rmo) == 1:
        return all_rmo[0]
    rmoA = all_rmo[0]
    rmoB = all_rmo[1]
    if keep_duplicates is False:
        stmt = session.query(rmoA).union(session.query(rmoB))
    else:
        stmt = session.query(rmoA).union_all(session.query(rmoB))

    for rmo in all_rmo[2:]:
        if keep_duplicates is False:
            stmt = stmt.union(session.query(rmo))
        else:
            stmt = stmt.union_all(session.query(rmo))

    #convert stmt as  subquery object
    stmt = stmt.subquery()

    if preserve_column_name is True:
        relabel_stmts = []
        original_col_names = rmoA.c.keys()
        anon_col_names = stmt.c.keys()
        for on, an in zip(original_col_names, anon_col_names):
            assert(on in an)
            relabel_stmts.append(getattr(stmt.c, an).label(on))
        return session.query(*relabel_stmts).subquery()

    return stmt


def cardinality(session=None, rmo=None):
    '''output: number of records in the rmo table'''
    return session.query(rmo).count()


def max(session=None, rmo=None, attr=None):
    '''output: returns the max value of the attr'''
    stmt = buildstmt(session, rmo)
    qry = session.query(func.max(getattr(stmt.c, attr)).label("max_val"))
    res = qry.one()
    max_val= res.max_val
    return max_val



def min(session=None, rmo=None, attr=None):
    '''output: returns the max value of the attr'''
    stmt = buildstmt(session, rmo)
    qry = session.query(func.min(getattr(stmt.c, attr)).label("min_val"))
    res = qry.one()
    min_val= res.min_val
    return min_val



def domain(session=None, rmo=None, attr=None):
    '''output: the range of values for the attr attribute of rmo, i.e, [max(attr), min(attr)]'''
    return [min(session, rmo, attr), max(session, rmo, attr)]


def enumerate(session=None, rmo=None, enumerate_attr_label=None):
    '''
    assign a serial number attribute to rmo. 

    developer's note: probably there is a simpler postgres udf for this, 
                      but for now using window function
    '''
    robject = buildstmt(session, rmo)
    stmt = session.query(func.row_number().over().label(enumerate_attr_label), robject).subquery()
    return stmt
                         

def flatten(session=None, rmo=None, flat_attr = None, uniq=True):
    '''
    creates a single column as union  of all the columns. 
    precondition: all columns have to be of the same type
    TODO: check for pre condition
    '''
    all_cols  = getColNames(session, rmo)
    for col in all_cols:
        all_stmts = vam.rename_attribute(session, sc.proj(session, rmo, [col]), col, flat_attr)
    
    stmt = set_union(session=session, all_rmo=all_stmts, keep_duplicates=False, preserve_column_name=True)
    if uniq is True:
        stmt = distinct(session, stmt, [flat_attr])
    return stmt
    
def window_rank(session=None, rmo=None, partition_attr=None, order_attr=None, attr_label=None, carry_over_attrl=None):
    '''
    applies a counter that restarts for each unique partition_attr, which increments for each in that group.
    TODO: for now only the partition_attr and order_attr are passed through the result. We need to maintain
    rest of the column.
    '''
    robject = buildstmt(session, rmo)
    #TODO: test window query
#     local_label = 'null_' + attr_label
#     stmt= session.query(func.row_number().over(order_by=getattr(robject.c, partition_attr), partition_by=getattr(robject.c, partition_attr)).label(local_label),  rmo).subquery() 
#     stmt = cast_integer(session, stmt, local_label,  attr_label)
# #    stmt= session.query(func.count(getattr(robject.c, order_attr)).over(order_by=getattr(robject.c, order_attr), partition_by=getattr(robject.c, partition_attr)).label(attr_label),  getattr(robject.c, order_attr)).subquery()

    local_label =  attr_label
    if carry_over_attrl is not None:
        cols_refs = getAttrList(robject, carry_over_attrl) 
        stmt = session.query(getattr(robject.c, partition_attr), getattr(robject.c, order_attr),(cast(func.row_number().over(order_by=desc(getattr(robject.c, order_attr)), partition_by=getattr(robject.c, partition_attr)),Integer).label(local_label)), *cols_refs).subquery()
    else:
        stmt = session.query(getattr(robject.c, partition_attr), getattr(robject.c, order_attr),(cast(func.row_number().over(order_by=getattr(robject.c, order_attr), partition_by=getattr(robject.c, partition_attr)),Integer).label(local_label))).subquery()
        #stmt= session.query(func.row_number().over(order_by=getattr(robject.c, partition_attr), partition_by=getattr(robject.c, partition_attr)).label(local_label)).subquery() 
    #stmt = sch.cast_integer(session, stmt, local_label,  attr_label)
    #print stmt.c.keys()
    #    stmt= session.query(func.count(getattr(robject.c, order_attr)).over(order_by=getattr(robject.c, order_attr), partition_by=getattr(robject.c, partition_attr)).label(attr_label),  getattr(robject.c, order_attr)).subquery()
#>>>>>>> 570358cd84f7dc032ee416c9c86b7d6d9eb2e749
    return stmt


def fuseRange(session=None, rmoA=None, rmoB=None, attrA=None, attrB_lb=None, attrB_ub=None):
    '''
    join rmoA with rmoB where
    rmoB.attrB_lb <rmoA.attrA < rmB.attrB_ub
    '''

    rmoA = buildstmt(session, rmoA)
    rmoB = buildstmt(session, rmoB)
    
    if (attrA == attrB_lb) or (attrA == attrB_ub):
        print("join attributes must have different names")
        assert(0)
    stmt=session.query(rmoA, rmoB, getattr(rmoA.c, attrA), getattr(rmoB.c, attrB_lb), getattr(rmoB.c, attrB_ub)).filter(getattr(rmoA.c, attrA) >= getattr(rmoB.c, attrB_lb)).filter(getattr(rmoA.c, attrA) <= getattr(rmoB.c, attrB_ub)).subquery()
    return stmt


def fuseEQ(session=None, rmoA=None, rmoB=None, attrA=None, attrB=None, preserve_columns=False):
    oobject = buildstmt(session, rmoA)
    iobject = buildstmt(session, rmoB)
    if attrB is None:
        attrB = attrA

    if(attrA == attrB):
        oobject = rename_attribute(session, oobject, attrA, 'o_' + attrA)
        iobject = rename_attribute(session, iobject, attrB, 'i_' + attrB)
        if preserve_columns:
            cols_ref = getAttrList(oobject, vam.getColNames(session, oobject)) + getAttrList(iobject, vam.getColNames(session, iobject))
            stmt=session.query(oobject, iobject, *cols_ref).filter(getattr(oobject.c, 'o_' + attrA)==getattr(iobject.c, 'i_' + attrB)).subquery()
        else:
            stmt=session.query(oobject, iobject, getattr(oobject.c, 'o_'+attrA), getattr(iobject.c, 'i_' + attrB)).filter(getattr(oobject.c, 'o_'+attrA)==getattr(iobject.c, 'i_'+attrB)).subquery()
        stmt = rename_attribute(session, stmt, 'o_' + attrA, attrA)
        
    else:
        if preserve_columns:
            cols_ref = getAttrList(oobject, vam.getColNames(session, oobject)) + getAttrList(iobject, vam.getColNames(session, iobject))
            stmt=session.query(oobject, iobject, *cols_ref).filter(getattr(oobject.c, attrA)==getattr(iobject.c, attrB)).subquery()
        else:
            stmt=session.query(oobject, iobject, getattr(oobject.c, attrA), getattr(iobject.c, attrB)).filter(getattr(oobject.c, attrA)==getattr(iobject.c, attrB)).subquery()
            
    return stmt


def fuseLTE(session=None, rmoA=None, rmoB=None, attrA=None, attrB=None, preserve_columns=False):
    '''
    merge with fuseEQ
    '''
    oobject = buildstmt(session, rmoA)
    iobject = buildstmt(session, rmoB)
    if attrB is None:
        attrB = attrA

    if(attrA == attrB):
        #print "join attributes must have different names"
        oobject = rename_attribute(session, oobject, attrA, 'o_' + attrA)
        iobject = rename_attribute(session, iobject, attrB, 'i_' + attrB)
        if preserve_columns:
            cols_ref = getAttrList(oobject, getColNames(session, oobject)) + getAttrList(iobject, getColNames(session, iobject))
            stmt=session.query(oobject, iobject, *cols_ref).filter(getattr(oobject.c, 'o_' + attrA) <= getattr(iobject.c, 'i_' + attrB)).subquery()
        else:
            stmt=session.query(oobject, iobject, getattr(oobject.c, 'o_'+attrA), getattr(iobject.c, 'i_' + attrB)).filter(getattr(oobject.c, 'o_'+attrA) <= getattr(iobject.c, 'i_'+attrB)).subquery()
        stmt = rename_attribute(session, stmt, 'o_' + attrA, attrA)
        
    else:
        if preserve_columns:
            cols_ref = getAttrList(oobject, getColNames(session, oobject)) + getAttrList(iobject, getColNames(session, iobject))
            stmt=session.query(oobject, iobject, *cols_ref).filter(getattr(oobject.c, attrA)<=getattr(iobject.c, attrB)).subquery()
        else:
            stmt=session.query(oobject, iobject, getattr(oobject.c, attrA), getattr(iobject.c, attrB)).filter(getattr(oobject.c, attrA)<=getattr(iobject.c, attrB)).subquery()
            
    return stmt
