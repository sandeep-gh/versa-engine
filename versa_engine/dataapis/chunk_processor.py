import versa_utils as vu
import versa_core.relational as re

mysession = None

def pool_initializer():
    global mysession
    mysession = vu.build_session()


    

    
def chunk_processor_type1(packed_arg):
    '''
    works directly on materialized data_rmo
    '''
    data_rmo, query_of, op, span = packed_arg
    offset, limit = span
    all_res = []
    for rec in mysession.query(data_rmo).slice(offset, limit).yield_per(300000):
         res_rec = op(rec)
         all_res.append(rec)

    return all_res


def chunk_processor_type2(packed_arg):
    '''
    defines a anchor_data_rmo and a query op. 
    the anchor_data_rmo is chunked and query is computed 
    on the chunk.
    Finally op is performed on the query(chunk(anchor_data_rmo))
    '''
    anchor_data_rmo, query_op, query_args, op, span = packed_arg
    [country] = query_args
    offset, limit = span
    anchor_data_rmo_chunk = mysession.query(anchor_data_rmo).slice(offset, limit).subquery()
    res_stmt = query_op([mysession, country, anchor_data_rmo_chunk])
    
    #with open("test_" + str(offset) + ".txt", "w") as fh:
    #    fh.write(str(re.cardinality(mysession, res_stmt)) + " \n " )
    
    all_res = []
    return all_res #as test run
    for rec in mysession.query(res_stmt).yield_per(300000):
        res_rec = op(rec)
        all_res.append(res_rec)
    return all_res
