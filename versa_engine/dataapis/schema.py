import sqlalchemy
from sqlalchemy import MetaData, Column, Integer, String, Table, Index, Interval, Float, VARCHAR
from  sqlalchemy.sql.expression import func, select, desc, over, cast, literal_column
from sqlalchemy.orm import class_mapper, defer, mapper
from sqlalchemy.sql.sqltypes import NullType
from string import Template
from  sqlalchemy.sql.expression import cast

import rmo.versa_api_meta as vam
from rmo.header import *
from rmo.versa_api_meta import getAttrList, getAttrList_but



def proj(session, object, attrlist):

    robject = vam.buildstmt(session, object)
    stmt=session.query(*getAttrList(robject,attrlist)).subquery()
    return stmt

def alias_attributes(session=None, rmo=None,  alias_prefix=None, attrl=None, aliased_labels=None):
    '''
    assign new names to the attribute/columns
    '''
    assert(alias_prefix is not None)
    robject = vam.buildstmt(session, rmo)
    all_attrl = vam.getColNames(session, robject)
    if attrl == None:
        attrl=all_attrl
    aliased_attrl = []
    for attr in all_attrl:
        if attr in attrl:
            aliased_attrl.append(getattr(robject.c, attr).label(alias_prefix + '_' + attr))
            if  aliased_labels is not None:
                aliased_labels.append([attr, alias_prefix + '_' + attr])
        else:
            aliased_attrl.append(getattr(robject.c, attr))
            
    return session.query(*aliased_attrl).subquery()


def select(session=None, cls=None):
    """Convert to subquery (rmo) object
    
    Args:
        cls: a sqlachemcy declarative base class
        
    Returns:
        A subquery (or rmo) object. 
    """
    stmt=session.query(cls).subquery()
    return stmt



# def limit(session=None, rmo=None, num_recs=None):
#     """Restrict the  number of records by num_recs
    
#     Args:
#         num_recs:
        
#     Returns:
#         A new rmo with only num_recs from the input rmo
#     """

#     return session.query(rmo).limit(num_recs).subquery()





def drop(session=None, rmo=None, attr=None):
    robject = rmo
    if(type(robject) == sqlalchemy.ext.declarative.DeclarativeMeta):
        robject = select(session, robject)
    selectedAttrList =  getAttrList_but(robject, [attr])
    if not selectedAttrList:
        return None
    stmt=session.query(*getAttrList(robject,selectedAttrList)).subquery()
    stmt = vam.carry_over_primary_keys(session, robject, stmt)
    return stmt

def cast_integer(session=None, rmo=None, attr=None, int_attr=None):
    robject = vam.buildstmt(session, rmo)
    stmt=session.query(robject, (cast(getattr(robject.c, attr), Integer)).label(int_attr)).subquery()
    return stmt

def cast_string(session=None, rmo=None, attr=None, string_attr=None, drop_attr=True):
    '''
    We currently don't support casting of an attribute that is the primary key. 
    It is easy to fix tough if need arises. 
    '''
    assert string_attr != attr

    robject = vam.buildstmt(session, rmo)
    robject_pkeys = vam.get_pkeys(session, robject)

    attr_ref = getattr(robject.c, attr)
    stmt=session.query(robject, (cast(attr_ref, String)).label(string_attr)).subquery()

    #drop attr from robject    
    if drop_attr:
        stmt = drop(session, stmt, attr)

    stmt = vam.carry_over_primary_keys(session, robject, stmt)

    if drop_attr:
        if attr in robject_pkeys:
            vam.set_primary_keys(session, stmt, [string_attr])
    return stmt


def add_const_column(session=None, rmo=None, val=None, label=None):
    '''add a constant attribute with label and val
       currently only literal columns are supported
       TODO: if val is string starting with  002; then for some reason it resolves to 2
    '''
    robject = vam.buildstmt(session, rmo)
    stmt = session.query(robject, literal_column(val, VARCHAR()).label(label)).subquery()
    return stmt

def add_float_const_column(session=None, rmo=None, val=None, label=None):
    '''add a constant attribute with label and val
       currently only literal columns are supported
       TODO: if val is string starting with  002; then for some reason it resolves to 2
    '''
    robject = vam.buildstmt(session, rmo)
    stmt = session.query(robject, cast(literal_column(val, Float()), Float).label(label)).subquery()
    return stmt

def add_integer_const_column(session=None, rmo=None, val=None, label=None):
    '''add a constant attribute with label and val
       currently only literal columns are supported
       TODO: if val is string starting with  002; then for some reason it resolves to 2
    '''
    robject = vam.buildstmt(session, rmo)
    stmt = session.query(robject, cast(literal_column(val, Integer()), Integer).label(label)).subquery()
    return stmt

# def get_rmo_attribute_types(rmo=None, use_type=None):
#     '''
#     use_type: how the types are  used, i.e., to write metadata  file, 
#     to write rmo file, or passed around in the current runtime
#     '''
#     types = [getattr(rmo.c, col).type for col in cols]
#     for idx, ctype in enumerate(types):
#         if type(ctype) == NullType:
#             types[idx] = String()
        
#             #try to format geometry colums like geometry("POINT")   
#         try:
#             #To work for Geography colums, you need to add +","+ctypes.type.srid
#             #types[idx] =str(ctype).split('(')[0] +  '("' +types[idx].geometry_type +'")'
#             if use_type === 'rmo_def':
#                 #go from geometry(MULTIPOLYGON,-1) ==> Geometry('MULTIPOLYGON', -1)
#                 pass
#             if use_type == 'metadata':
#                 #go from geometry(MULTIPOLYGON,-1) ==> polygon
#                 pass
#             else:
#                 #return as it is

    
def create_dbschema_for_rmo(session=None, rmo=None, tbl_name=None, cls_name=None, indexes=[], cls_fn=None, pks=None, is_temporary=True, return_rmo=False):
    """
    Create table tbl_name and save the content of an rmo in the table.

    Args:
         cls: is the class-reference to the tables
         indexes: list of cols on which to create index
         cls_fn: file in which the class definition is written out
         pks: list of columns that form the primary key

    Output: 
         cls: reference to the class that maps to the table

    Usage: 

    """
    #print "rmo= ", rmo
    cols = vam.getColNames(session, rmo)
    types = [getattr(rmo.c, col).type for col in cols]
    
    #covert NullType to String() -- nullTypes are created by literal strings
    for idx, ctype in enumerate(types):
        if type(ctype) == NullType:
            types[idx] = String()
    

    if pks is None:
        pks = [getattr(rmo.c, col).primary_key for col in cols]
    else:
        pks = [col in pks for col in cols]
    if indexes is None:
        indexes = pks
    else:
        indexes = [col in indexes for col in cols]

    m_cls = create_mapped_table(session, tbl_name, cls_name, cols, types, pks, indexes)
    
    #m_rmo = select(session, m_cls)
    #write_rmo_def(session, m_rmo, m_cls,  tbl_name, cls_fn)
    return  m_cls  #return the class or the rmo

def build_metadata_for_rmo(session=None, rmo=None, model_name=None, metadata_fn=None, delimiter='comma'):
    '''
    '''
    rmo=vam.buildstmt(session, rmo)
    #these codes need to be refactored out
    cols = vam.getColNames(session, rmo)
    types = [getattr(rmo.c, col).type for col in cols]
    #covert NullType to String() -- nullTypes are created by literal strings
    for idx, ctype in enumerate(types):
        if type(ctype) == NullType:
            types[idx] = String()
        
            #try to format geometry colums like geometry("POINT")   
        try:
            #To work for Geography colums, you need to add +","+ctypes.type.srid
            types[idx] = types[idx].geometry_type.lower()+"_2d"# str(ctype).split('(')[0] +  '("' +types[idx].geometry_type +'")'
        except:
            #It isn't a geography colum, so nothing should change
            pass

    pks = [getattr(rmo.c, col).primary_key for col in cols] #pks as list of bools
    pks = [col for col,flag in zip(cols,pks) if flag] #get pks as list of cols
    #refactor uptil here

    cols_md_str=''
    for col,ctype in zip(cols, types):
        cols_md_str += Template('<column><name>${col}</name><type>$ctype</type></column>\n').substitute(locals())

    md_prefix="<metadata>\n<model>${model_name}</model>\n<delimiter>${delimiter}</delimiter>\n<header>False</header>\n<columns>\n"
    pk_keys_str = ''
    for pk in pks:
        pk_keys_str += "<key>"+pk +"</key>\n"

    md_postfix="</columns>\n<primarykey>\n$pk_keys_str\n</primarykey>\n</metadata>"
    md_str=Template(md_prefix + cols_md_str + md_postfix).substitute(locals())
    if metadata_fn is  None:
        metadata_fn =   model_name + '.md'
    with open(metadata_fn, 'w+') as md_fh:
        md_fh.write(md_str)


def create_mapped_table(session=None, tbl_name=None, cls_name=None, attrs=None, types=None, primary_keys=None, indexes=None, is_temporary=False):
    '''attrs: labels for the columns
       types: a list of types, i.e.  [String(), Integer(), String()]
       primary_keys: a list of bools
       indexes: a list of bools
       
       is_temporary is false by default because it is failing on geometry objects
       '''
    metadata=MetaData() 
    prefixes = []
    if is_temporary:
        prefixes.append('TEMPORARY')

    
    
    
    table = Table(tbl_name, metadata, 
              *(Column(col, ctype, primary_key=pk, index=idx) for  col, ctype, pk, idx in zip(attrs, types, primary_keys, indexes)),
                  prefixes=prefixes)
    try:
        table.create(session.connection().engine, checkfirst=True)
    except Exception as  e:
        print("Cannot create table: %s" % e)

    mydict={'__tablename__':tbl_name, '__table_args__': ({'autoload':True},), '__table__': table}

    #mydict={'__tablename__':tbl_name, '__table__': table}
    cls = type(cls_name, (Base,), mydict)
    for attr, pk in zip(attrs, primary_keys):
        col_hn = getattr(cls, attr)
        col_hn.primary_key = pk

    #mapper(cls, table)
    #satisfying versa
    session.commit()
    return cls

