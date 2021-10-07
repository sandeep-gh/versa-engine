import versa_core.schema as sc
import versa_core.relational as re
import versa_range_api as vra
import versa_api_meta as vam
import versa_api_join_list as vajl

#def restrictEQ(session, superset_rmo=None, key_rmo=None, superset_attrl=None, key_attrl=None, proj_attrl=None):


def restrictContains(session, superset_rmo=None, key_rmo=None, superset_attr=None, key_attr=None, proj_attrl=None):
    '''
    comment: we should be using left join
    '''
    stmt = vra.fuseContains(session, superset_rmo, key_rmo, superset_attr, key_attr)
    if proj_attrl is None:
        proj_attrl = vam.getColNames(session, superset_rmo)
        

    stmt = re.distinct(session, sc.proj(session, stmt, proj_attrl), proj_attrl)
    return stmt

def restrict(session, superset_rmo=None, key_rmo=None, superset_attr=None, key_attr=None, proj_attrl=None, make_distinct=False):
    '''
    comment: we should be using left join
    '''
    stmt = re.fuseEQ(session, superset_rmo, key_rmo, superset_attr, key_attr)
    if proj_attrl is None:
        proj_attrl = vam.getColNames(session, superset_rmo)
        
    stmt = sc.proj(session, stmt, proj_attrl)

    if make_distinct:
        stmt = re.distinct(session,stmt, proj_attrl)
    return stmt

def restrict_list(session, superset_rmo=None, key_rmo=None, superset_attrl=None, key_attrl=None, proj_attrl=None):
    '''
    the list of version of restrict
    comment: we should be using left join
    keep records where superset_rmo[superset_attrl] \in key_rmo[key_attrl]
    '''
    all_superset_attrl = vam.getColNames(session, superset_rmo)
    stmt = vajl.fuseEQ(session, superset_rmo, key_rmo, superset_attrl, key_attrl)
    if proj_attrl is not None:
        stmt = re.distinct(session, sc.proj(session, stmt, proj_attrl), proj_attrl)
    else:
        stmt = sc.proj(session, stmt, all_superset_attrl)
    return stmt
    
    

# def fuseAnnotate(session, base_rmo=None, data_rmo=None, join_attrl_base=None join_attrl_data=None, annotate_attrl=None, join_op_type= None, crop=False):
#     '''
#     goal is to keep the join_attrl in the background
#     crop will only keep the base_rmo primary key and the annotated 
#     join_op_type = ['contains', 'EQ', 'lt', 'gt']
#     '''
    
