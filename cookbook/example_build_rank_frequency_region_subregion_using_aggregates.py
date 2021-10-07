import versa_impl as vi
import versa_api as vapi
import versa_api_list as vapil
import versa_api_join_list as vapjl
from Global_Food_Prices_model  import *


session = vi.init('v2')
subregions_by_regions = vapi.distinct(session, vapi.proj(session, Global_Food_Prices, ['adm0_id', 'adm1_id']), ['adm0_id', 'adm1_id'])
region_sz_by_region = vapi.aggregate(session, subregions_by_regions, 'adm0_id', 'region_sz')
num_regions_by_region_sz = vapi.aggregate(session, region_sz_by_region, 'region_sz', 'num_regions')
print num_regions_by_region_sz.c.keys()
num_regions_by_region_sz_ordered = vapil.ascending(session, num_regions_by_region_sz, ['region_sz', 'num_regions'])
num_regions_by_region_sz_array   = vapi.build_array(session, num_regions_by_region_sz_ordered, ['region_sz', 'num_regions'])

print "c= ", num_regions_by_region_sz_array.c.keys()

restype={'x_attr':'array_region_sz', 'y_attr':'array_num_regions'}
restype['plot'] = {'title': 'distribution_subregions_per_regions',
                   'xlabel': '#subregion/regions',
                   'ylabel': '#regions',
                   'ylog' : 'False'
                   }

vapi.plot_XY(session=session, stmt=num_regions_by_region_sz_array, restype=restype)






