import sqlalchemy as sa
from sqlalchemy.sql import column, tuple_
from sqlalchemy import func
from sqlalchemy.sql.expression import cast
from dataapis.ranges.range import IntRangeType,NumericRangeType
from intervals import IntInterval
from geoalchemy2.functions import ST_Contains, ST_Covers
import geoalchemy2 as ga2

from rmo.versa_api_meta import buildstmt
#import versa_api_join_list as vajl
import rmo.versa_api_meta as vam




#unary apis
def equals(session=None, rmo=None, range_attr=None, arg_range=None):
    '''return all tuples where range_attr equals arg_range'''
    robject = buildstmt(session, rmo)
    stmt = session.query(robject).filter(getattr(robject.c, range_attr)==arg_range).subquery()
    return stmt

def lengthEquals(session=None, rmo=None, range_attr=None, arg_range=None):
    '''return all tuples where range_attr length is equal to arg_range'''
    robject = buildstmt(session, rmo)
    stmt = session.query(robject).filter(getattr(robject.c, range_attr).length == arg_range).subquery()
    return stmt


def findParents(session=None, rmo=None, range_attr=None, arg_range=None):
    '''return all tuples that are parents of  arg_range.  Currently contains tuples that are equal.'''
    robject = buildstmt(session, rmo)
    stmt = contains(session, rmo, range_attr,arg_range)
    return stmt

def findChildren(session=None, rmo=None, range_attr=None, arg_range=None):
    '''return all tuples that are children of  arg_range.  Currently contains tuples that are equal.'''
    robject = buildstmt(session, rmo)
    stmt = containedBy(session, rmo, range_attr,arg_range)
    return stmt

def findBetween(session=None, rmo=None, range_attr=None, parent_range=None, child_range=None):
    '''return all tuples that are between parent_range and child_range.  Currently contains tuples that are equal to both.'''
    robject = buildstmt(session, rmo)
    stmt = findChildren(session, rmo, range_attr,parent_range)
    stmt = findParents(session, stmt, range_attr,child_range)
    return stmt

def findSharedParents(session=None, rmo=None, range_attr=None, arg_range1=None, arg_range2=None):
    '''return all tuples that are parents of arg_range1 and arg_range2'''
    robject = buildstmt(session, rmo)
    stmt = findParents(session, rmo, range_attr,arg_range1)
    stmt = findParents(session, stmt, range_attr, arg_range2)
    return stmt

def findMostRecentParent(session=None, rmo=None, range_attr=None, arg_range1=None, arg_range2=None):
    '''return all smallest tuple that contains both arg_range1 and arg_range2'''
    robject = buildstmt(session, rmo)
    stmt = findParents(session, rmo, range_attr,arg_range1)
    stmt = findParents(session, stmt, range_attr, arg_range2)
    stmt = ascending(session, stmt, range_attr)
    stmt = limit(session, stmt, 1)
    return stmt

def contains(session=None, rmo=None, range_attr=None, arg_range=None):
    '''return all tuples where intv_attr is contained in arg_range (see findParents)'''
    robject = buildstmt(session, rmo)
    stmt = session.query(robject).filter(getattr(robject.c, range_attr).contains(arg_range)).subquery()
    return stmt

def overlaps(session=None, rmo=None, range_attr=None, arg_range=None):
    '''return all tuples where intv_attr is contained in arg_range (see findParents)'''
    robject = buildstmt(session, rmo)
    stmt = session.query(robject).filter(getattr(robject.c, range_attr).op("&&")(arg_range)).subquery()
    return stmt

def clip(session=None, rmo=None, range_attr=None, arg_range=None, result_attr=None):
    '''return all tuples where intv_attr is contained in arg_range (see findParents)'''
    robject = buildstmt(session, rmo)
    arg_range_str = arg_range
    if isinstance(arg_range_str, (list, tuple)):
        arg_range_str = '[' + str(arg_range[0]) +"," + str(arg_range[1]) + ']'

    stmt = session.query(robject, (getattr(robject.c, range_attr).op("*")(arg_range_str)).label(result_attr)).subquery()
    return stmt


def intersects(session=None, rmo=None, range_attr=None, arg_range=None):
    '''return all tuples where intv_attr is contained in arg_range (see findParents)'''
    robject = buildstmt(session, rmo)
    stmt = session.query(robject).filter(getattr(robject.c, range_attr).op("*")(arg_range)).subquery()
    return stmt


def containedBy(session=None, rmo=None, range_attr=None, arg_range=None):
    '''return all tuples where intv_attr contains arg_range (see  findChildren)'''
    robject = buildstmt(session, rmo)
    stmt = session.query(robject).filter(getattr(robject.c, range_attr).contained_by(arg_range)).subquery()
    return stmt

def createRange(session=None, rmo=None, lower_attr=None, upper_attr=None, range_attr=None):
    '''creates a range given an upper and lower bound'''
    lower = sa.cast(getattr(rmo.c, lower_attr), sa.String)
    upper = sa.cast(getattr(rmo.c, upper_attr), sa.String)
    robject = buildstmt(session, rmo)
    stmt = session.query(rmo, sa.cast(sa.func.concat('[' + lower+','+upper+']'), IntRangeType).label(range_attr)).subquery()
    return stmt

def fuseRangeOverlaps(session=None, rmoA=None, rmoB=None, attrA=None, attrB=None, preserve_columns=False):
    oobject = buildstmt(session, rmoA)
    iobject = buildstmt(session, rmoB)
    if(attrA == attrB):
        print("join attributes must have different names")
    else:
        if preserve_columns:
            cols_ref = vam.getAttrList(oobject, vam.getColNames(session, oobject)) + vam.getAttrList(iobject, vam.getColNames(session, iobject))
            stmt=session.query(oobject, iobject, *cols_ref).filter(getattr(oobject.c, attrA).op("&&")(getattr(iobject.c, attrB))).subquery()
        else:
            stmt=session.query(oobject, iobject, getattr(oobject.c, attrA), getattr(iobject.c, attrB)).filter(getattr(oobject.c, attrA).op("&&")(getattr(iobject.c, attrB))).subquery()
    return stmt



#stmt = vra.createTimePeriod(session, rmo=Department, attr_range='range', ranges=[[0,400],[401,600]], labels=["Summer","Winter"],tableName = "Table17", clsName = 'clss')
def createTimePeriod(session=None, ranges=None, labels=None, tableName=None, clsName = None, clsFn=None):
    '''
    modName is the name of python file where the rmo defintion is stored
    '''
    [t_timeperiod, TimePeriod, timeperiod_rmo] = vapi.create_table_and_rmo(session=session, tbl=tableName, cls=clsName, attrs=["seq_id", "time_period_range","time_period_name"], types=[sa.Integer, IntRangeType, sa.String], primary_keys=[True, False,False], indexes=[True, True,False])
    for idx, [range, name]  in enumerate(zip(ranges, labels)):
        timePeriod = TimePeriod()
        timePeriod.seq_id = idx
        timePeriod.time_period_range=("["+str(range[0])+","+str(range[1])+"]")
        timePeriod.time_period_name= name
        session.add(timePeriod)
    
    vapi.write_rmo_def(session, timeperiod_rmo, cls_name=clsName, tbl_name=tableName, cls_fn=clsFn)
    return timeperiod_rmo

def partition_int_range(session=None,int_range=None,  table_name=None, partition_attr_label='range', partition_span=None, ):
    '''
    create a table/dataset of ranges of size range_step, starting from range_start_idx and end at range_end_idx.
    
    '''

    rmo_name = 'c_' + table_name
    [t_timeperiod, TimePeriod, timeperiod_rmo] = vapi.create_table_and_rmo(session=session, tbl=table_name, cls=rmo_name, attrs=["seq_id", partition_attr_label], types=[sa.Integer, IntRangeType], primary_keys=[True, False], indexes=[True, True])

    for val in range(int_range[0], int_range[1], partition_span):
        timePeriod = TimePeriod()
        lb = val
        ub = val + partition_span
        timePeriod.__dict__[partition_attr_label]=("["+str(lb)+","+str(ub)+"]")
        session.add(timePeriod)

    cls_fn = table_name + "_model.py"
    vapi.write_rmo_def(session, timeperiod_rmo, cls_name=rmo_name, tbl_name=table_name, cls_fn=cls_fn)
    return timeperiod_rmo


    

def rangeIntersect_attrs(session=None, rmo=None, attrA=None, attrB=None, intersect_label='intersect_range'):
    object = buildstmt(session, rmo)
    stmt=session.query(object, (getattr(object.c, attrA).op("*")(getattr(object.c, attrB))).label(intersect_label)).subquery()
    return stmt


def appendRangeLength(session=None, rmo=None, range_attr=None, length_label=None):
    object = buildstmt(session, rmo)
    range_attr_handle = getattr(object.c, range_attr)
    stmt = session.query(object, (func.upper(range_attr_handle) - func.lower(range_attr_handle)).label(length_label)).subquery()
    return stmt


def fuseContains(session=None, rmo_outer=None, rmo_inner=None, outer_attr=None, inner_attr=None, preserve_columns=False):
    ''' 
	fuses 2 tables by checking if inner_attr is contained by outer_attr

	Geometry A contains Geometry B iff no points of B lie in the exterior of A, and at least one point of the interior of B lies in the interior of A	
        (If unsure of contains, or covers, you should use fuseContains)
    '''
    
    oobject = buildstmt(session, rmo_outer)
    iobject = buildstmt(session, rmo_inner)
    if preserve_columns:
        cols_ref = vam.getAttrList(oobject, vam.getColNames(session, oobject)) + vam.getAttrList(iobject, vam.getColNames(session, iobject))
        stmt=session.query(oobject, iobject, *cols_ref).filter(getattr(oobject.c, outer_attr).contains(getattr(iobject.c, inner_attr))).subquery()
    else:
        stmt=session.query(oobject, iobject, getattr(oobject.c, outer_attr), getattr(iobject.c, inner_attr)).filter(getattr(oobject.c, outer_attr).ST_Contains(getattr(iobject.c, inner_attr))).subquery()
    return stmt

def fuseBoundingBoxContains(session=None, rmo_outer=None, rmo_inner=None, outer_attr=None, inner_attr=None, preserve_columns=False):
    ''' 
         fuses 2 tables by checking if inner_attr is contained by outer_attr's bounding box
    '''
    oobject = buildstmt(session, rmo_outer)
    iobject = buildstmt(session, rmo_inner)
    if preserve_columns:
        cols_ref = vam.getAttrList(oobject, vam.getColNames(session, oobject)) + vam.getAttrList(iobject, vam.getColNames(session, iobject))
        stmt=session.query(oobject, iobject, *cols_ref).filter(getattr(oobject.c, outer_attr).contains(getattr(iobject.c, inner_attr))).subquery()
    else:
        stmt=session.query(oobject, iobject, getattr(oobject.c, outer_attr), getattr(iobject.c, inner_attr)).filter(getattr(oobject.c, outer_attr).contains(getattr(iobject.c, inner_attr))).subquery()
    return stmt
def fuseCovers(session=None, rmo_outer=None, rmo_inner=None, outer_attr=None, inner_attr=None, preserve_columns=False):
    ''' 
        fuses 2 tables by checking if inner_attr is convered by outer_attr

	Geometry A covers Geometry B iff no points of B lie in the exterior of A
    '''
    oobject = buildstmt(session, rmo_outer)
    iobject = buildstmt(session, rmo_inner)
    if preserve_columns:
        cols_ref = vam.getAttrList(oobject, vam.getColNames(session, oobject)) + vam.getAttrList(iobject, vam.getColNames(session, iobject))
        stmt=session.query(oobject, iobject, *cols_ref).filter(getattr(oobject.c, outer_attr).contains(getattr(iobject.c, inner_attr))).subquery()
    else:
        stmt=session.query(oobject, iobject, getattr(oobject.c, outer_attr), getattr(iobject.c, inner_attr)).filter(getattr(oobject.c, outer_attr).ST_Covers(getattr(iobject.c, inner_attr))).subquery()
    return stmt

def fuseCoversUnique(session=None, rmo_outer=None, rmo_inner=None, outer_attr=None, inner_attr=None, unique_attr_list=None,counterColumn=None, preserve_columns=False):
    ''' 
        fuses 2 tables by checking if inner_attr is convered by outer_attr.  Removes any duplicate points (based on unique_attr).

        Geometry A covers Geometry B iff no points of B lie in the exterior of A
    '''
    oobject = buildstmt(session, rmo_outer)
    iobject = buildstmt(session, rmo_inner)
    if counterColumn is None:
        counterColumn = unique_attr_list[0] + '_seq'
    if preserve_columns:
        cols_ref = vam.getAttrList(oobject, vam.getColNames(session, oobject)) + vam.getAttrList(iobject, vam.getColNames(session, iobject)
)
        stmt=session.query(oobject, iobject, *cols_ref).filter(getattr(oobject.c, outer_attr).ST_Covers(getattr(iobject.c, inner_attr))).subquery()
    else:
        stmt=session.query(oobject, iobject, getattr(oobject.c, outer_attr), getattr(iobject.c, inner_attr)).filter(getattr(oobject.c, outer_attr).ST_Covers(getattr(iobject.c, inner_attr))).subquery()

    query2 = vapil.subgroup_count(session, stmt, unique_attr_list,attr_label=counterColumn)
    final = vapi.filterEQ(session,query2,attr=counterColumn,value=1)
    return final

def fuseRangeIntersects(session=None, rmoA=None, rmoB=None, attrA=None, attrB=None, preserve_columns=False, intersect_label='intersect_range'):
    '''
    join rmoA with rmoB where
    rmoB.attrB intersects with rmoA.attrA
    '''
    oobject = buildstmt(session, rmoA)
    iobject = buildstmt(session, rmoB)
    if(attrA == attrB):
        print("join attributes must have different names")
    else:
        if preserve_columns:
            cols_ref = vam.getAttrList(oobject, vam.getColNames(session, oobject)) + vam.getAttrList(iobject, vam.getColNames(session, iobject))
            stmt=session.query(oobject, iobject, *cols_ref).filter(getattr(oobject.c, attrA).op("*").getattr(iobject.c, attrB)).subquery()
        else:
            stmt=session.query(oobject, iobject, getattr(oobject.c, attrA), getattr(iobject.c, attrB)).filter(getattr(oobject.c, attrA).op("*")(getattr(iobject.c, attrB))).subquery()
    return stmt


def appendRangeFromStartAndDuration(session = None, rmo = None, st_attr = None, duration_attr = None, et_attr=None, range_attr = None):
    """
    creates a range from a start and duration [st_attr, st_attr+duration_attr]
    """
    object = buildstmt(session, rmo)
    stmt = session.query(object, (getattr(object.c, st_attr) + getattr(object.c, duration_attr)).label(et_attr)).subquery()
    stmt = session.query(stmt, cast(func.concat('[', cast(getattr(stmt.c, st_attr), sa.String), ',' , cast(getattr(stmt.c, et_attr), sa.String) , ']'), IntRangeType).label(range_attr)).subquery()
    return stmt

#helper functions
def ascending(session=None, rmo=None, attr=None):
    """
    sorts an attr in ascending order (possibly duplicated and can be deleted?)
    """
    robject = buildstmt(session, rmo)
    stmt=session.query(robject).order_by(getattr(robject.c, attr)).subquery()
    return stmt

def limit(session=None, rmo=None, num_recs=None):
    """Restrict the  number of records
    
    Args:
        num_recs: number of records to be returned
        
    Returns:
        A new rmo with only num_recs from the input rmo
    """
    return session.query(rmo).limit(num_recs).subquery()


#@redirect_recast:TODO
def createMultipleBoundingBox(session=None, rmo=None, agg_on_lon=None, agg_on_lat=None, agg_by_attr=None,bb_attr = None):
    '''
    aggregates the longitude and latitude by agg_by_attr, and saves it as 'min_'+lat, 'max_'+lon...
    Then creates a bounding box on each aggregated group.
    '''
 
    robject = buildstmt(session, rmo)

    stmt = session.query(func.min(getattr(robject.c, agg_on_lon)).label('min_'+agg_on_lon), func.max(getattr(robject.c, agg_on_lon)).label('max_'+agg_on_lon), func.min(getattr(robject.c, agg_on_lat)).label('min_'+agg_on_lat), func.max(getattr(robject.c, agg_on_lat)).label('max_'+agg_on_lat), getattr(robject.c, agg_by_attr).label(agg_by_attr)).group_by(getattr(robject.c, agg_by_attr)).subquery()

    stmt = session.query(stmt, cast(sa.func.concat('POLYGON((', 
cast(getattr(stmt.c, 'min_'+agg_on_lon), sa.String), ' ' , cast(getattr(stmt.c, 'min_'+agg_on_lat), sa.String) , ',' ,
cast(getattr(stmt.c, 'min_'+agg_on_lon), sa.String), ' ' , cast(getattr(stmt.c, 'max_'+agg_on_lat), sa.String) , ',' ,
cast(getattr(stmt.c, 'max_'+agg_on_lon), sa.String), ' ' , cast(getattr(stmt.c, 'max_'+agg_on_lat), sa.String) , ',' ,
cast(getattr(stmt.c, 'max_'+agg_on_lon), sa.String), ' ' , cast(getattr(stmt.c, 'min_'+agg_on_lat), sa.String) , ',' ,
cast(getattr(stmt.c, 'min_'+agg_on_lon), sa.String), ' ' , cast(getattr(stmt.c, 'min_'+agg_on_lat), sa.String) , '))'), ga2.types.Geometry).label(bb_attr)).subquery()
    return stmt
