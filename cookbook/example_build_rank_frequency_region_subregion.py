import versa_impl as vi
import versa_api as vapi
import versa_api_list as vapil
import versa_api_join_list as vapjl
from Global_Food_Prices_model  import *


session = vi.init('v2')
num_regions_by_region_sz_ordered = vapi.cardinality_distribution(session, Global_Food_Prices, agg_key='adm0_id', agg_on='adm1_id')
num_regions_by_region_sz_array   = vapi.build_array(session, num_regions_by_region_sz_ordered, ['capacity', 'count'])


restype={'x_attr':'array_capacity', 'y_attr':'array_count'}
restype['plot'] = {'title': 'distribution_subregions_per_regions_using_adv_api',
                   'xlabel': '#subregion/regions',
                   'ylabel': '#regions',
                   'ylog' : 'False'
                   }

vapi.plot_XY(session=session, stmt=num_regions_by_region_sz_array, restype=restype)








