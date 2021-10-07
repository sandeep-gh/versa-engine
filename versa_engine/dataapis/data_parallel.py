import utilities

from multiprocess import Pool
import versa_core.relational as re
import itertools
<<<<<<< HEAD
pool_sz = 8
=======
pool_sz = 2
>>>>>>> 570358cd84f7dc032ee416c9c86b7d6d9eb2e749


from chunk_processor import  chunk_processor_type1, chunk_processor_type2, pool_initializer


pool =  Pool(pool_sz, initializer=pool_initializer, initargs=() )

def process_data_parallel(session, data_rmo, op):
    '''
    computes op(r) for each r in data_rmo
    span is pair (offset, limit) that defines
    the range of processing
  
    data_rmo is a table with records as argument to op
    '''
    num_chunks = pool_sz * pool_sz    
    num_recs = re.cardinality(session, data_rmo)
    chunks = utilities.partition_in_chunks(num_recs, num_chunks)
    packed_args = zip([data_rmo] * num_chunks, [op] * num_chunks, chunks)
    #print pack_args
    #my_res = pool.map(chunk_processor, [data_rmo] * num_chunks, [op] * num_chunks, chunks)
    all_chunk_res = pool.map(chunk_processor_type1, packed_args)
    res = [x for chunk_res in  all_chunk_res for x in chunk_res]
    return res

def process_data_parallel_type2(session, anchor_data_rmo, query_op, query_args, rec_op):
    '''
    
    computes rec_op(query_op(query_args)) for each r in anchor_data_rmo
    span is pair (offset, limit) that defines
    the range of processing
    query_args is required to bypass a pickling bug

    '''
<<<<<<< HEAD
    num_chunks = pool_sz  * pool_sz
=======
    num_chunks = pool_sz * pool_sz    
>>>>>>> 570358cd84f7dc032ee416c9c86b7d6d9eb2e749
    num_recs = re.cardinality(session, anchor_data_rmo)
    chunks = utilities.partition_in_chunks(num_recs, num_chunks)
    packed_args = zip(itertools.repeat(anchor_data_rmo), itertools.repeat(query_op), itertools.repeat(query_args), itertools.repeat(rec_op), chunks)
    #print pack_args
    #my_res = pool.map(chunk_processor, [data_rmo] * num_chunks, [op] * num_chunks, chunks)
    all_chunk_res = pool.map(chunk_processor_type2, packed_args)
    res = [x for chunk_res in  all_chunk_res for x in chunk_res]
    return res
    
    
