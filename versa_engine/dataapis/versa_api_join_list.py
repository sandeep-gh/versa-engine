from versa_api_meta import *

def fuseEQ(session, roobject, riobject, ojattrl, ijattrl, relable_common_attr=True):
    '''
    '''
    #we are now allowing common attributes


    assert(len(ojattrl) == len(ijattrl))
    oobject = buildstmt(session, roobject)
    iobject = buildstmt(session, riobject)

    oattrl = getColNames(session, oobject)
    iattrl = getColNames(session, iobject)

    for oattr in ojattrl:
        assert oattr in oattrl
    
    for iattr in ijattrl:
        assert iattr in iattrl

    comm_attrs = set(oattrl) & set(iattrl)
    join_common_attrs = set(ojattrl) & set(ijattrl)
    nonjoin_common_attrs = comm_attrs - join_common_attrs

    if nonjoin_common_attrs:
        print("versa join: common attributes will be renamed")
        
    
    if not relable_common_attr:
        if len(nonjoin_common_attrs) > 0:
            print("cannot join objects with common attribute; turn relable_comm_attr=True to allow relabeling the column names")
            print(comm_cols)
            assert(0)



    
    o_join_distinct_attrs = set(ojattrl) - join_common_attrs
    o_nonjoin_distinct_attrs = set(oattrl) - ( set(ojattrl) | nonjoin_common_attrs)

    o_join_common_map = {}
    o_join_condition_attr_refs = [None] * len(ojattrl)    
    o_join_distinct_attr_refs = []
    #non join attributes that are part of the output
    keep_attr_refs = []

    for attr in oattrl:
        attr_h = getattr(oobject.c, attr)
        if attr in join_common_attrs: #this is the one we will
            attr_h = attr_h.label('o_' + attr) #relabel the common attribute
            o_join_common_map[attr] = attr_h #keep a map since we have to relabel it later
            o_join_condition_attr_refs[ojattrl.index(attr)] = attr_h
        if attr in o_join_distinct_attrs:
            o_join_distinct_attr_refs.append(attr_h)
            #o_join_condition_attr_refs.append(attr_h)
            o_join_condition_attr_refs[ojattrl.index(attr)] = attr_h
        if attr in nonjoin_common_attrs:
            attr_h = attr_h.label('o_' + attr) #relabel common attribute
            keep_attr_refs.append(attr_h)
        if attr in o_nonjoin_distinct_attrs:
            keep_attr_refs.append(attr_h)
    
    i_join_distinct_attrs = set(ijattrl) - join_common_attrs
    i_nonjoin_distinct_attrs = set(iattrl) - ( set(ijattrl) | nonjoin_common_attrs)
    i_join_condition_attr_refs = [None] * len(ojattrl)   
    i_join_distinct_attr_refs = []
    for attr in iattrl:
        attr_h = getattr(iobject.c, attr)
        if attr in join_common_attrs: #this is the one we will
            attr_h = attr_h.label('i_' + attr)
            i_join_condition_attr_refs[ijattrl.index(attr)] = attr_h
        if attr in i_join_distinct_attrs:
            i_join_distinct_attr_refs.append(attr_h)
            i_join_condition_attr_refs[ijattrl.index(attr)] = attr_h
        if attr in nonjoin_common_attrs:
            attr_h = attr_h.label('i_' + attr)
            keep_attr_refs.append(attr_h)
        if attr in i_nonjoin_distinct_attrs:
            keep_attr_refs.append(attr_h)

    #relable common join attributes
    o_join_common_attr_relabeled_refs = []
    for comm_attr in o_join_common_map.keys():
        attr_h = o_join_common_map[comm_attr]
        attr_h = attr_h.label(comm_attr)
        o_join_common_attr_relabeled_refs.append(attr_h)
        

    #res attrs == keep_attr_refs ({o,i}-distinct-nonjoin + {o,i}-common-non-join)
    all_res_attrs = keep_attr_refs + o_join_common_attr_relabeled_refs + o_join_distinct_attr_refs + i_join_distinct_attr_refs
    
    stmt = session.query(*all_res_attrs)

    #primary-key-lost-in-su bug fix
    primary_keys = []
    for attr_h in all_res_attrs:
        if attr_h.primary_key:
            primary_keys.append(attr_h.key)



    #finger crossed, insahalla, may-the-force-be-with-you
    #add join condition
    conditions=[]
    for oref,iref in zip(o_join_condition_attr_refs, i_join_condition_attr_refs):
        conditions.append(oref == iref)

    stmt = stmt.filter(*conditions)
    stmt = stmt.subquery()
    #bug: we are loosing primary key, when doing subquery operation, we need to file a bug
    #till then... here is  workaround
    for key in stmt.c.keys():
        attr_h = getattr(stmt.c, key)
        attr_label = attr_h.key
        if attr_label in primary_keys:
            attr_h.primary_key = True
            
    return stmt
