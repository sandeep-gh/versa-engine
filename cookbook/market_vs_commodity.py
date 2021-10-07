from rmo.versa_header import *
from globalfoodprices_model  import *


#session = vi.init('v2')
fh = open('global_food_prices_scatter_distribution.tex', 'w+')
commodities_by_regions = re.distinct(session, sc.proj(session, globalfoodprices, ['adm0_id', 'adm1_id', 'cm_id']), ['adm0_id', 'adm1_id', 'cm_id'])
num_commodities_by_regions = re.aggregate_count(session, commodities_by_regions, ['adm0_id','adm1_id'], 'num_commodities')
#print(re.cardinality(session, num_commodities_by_regions))

markets_by_regions = re.distinct(session, sc.proj(session, globalfoodprices, ['adm0_id', 'adm1_id', 'mkt_id']), ['adm0_id', 'adm1_id', 'mkt_id'])

num_markets_by_regions = re.aggregate_count(session, markets_by_regions, ['adm0_id','adm1_id'], 'num_markets' )
#print(re.cardinality(session, num_markets_by_regions))

num_markets_vs_num_commodity_by_regid = vapjl.fuseEQ(session, num_commodities_by_regions, num_markets_by_regions, ['adm0_id','adm1_id'], ['adm0_id','adm1_id'], relable_common_attr='True')
region_labels=  re.distinct(session, globalfoodprices, ['adm0_id', 'adm1_id', 'adm0_name', 'adm1_name'])
with_region_labels = vapjl.fuseEQ(session, region_labels, num_markets_vs_num_commodity_by_regid, ['adm0_id', 'adm1_id'], ['adm0_id', 'adm1_id'])
num_commodities_num_markets_by_regions = sc.proj(session, with_region_labels, ['num_commodities', 'num_markets', 'adm0_name', 'adm1_name'])
num_commodities_num_markets_by_region_label = re.conc_attrs(session, num_commodities_num_markets_by_regions, 'adm0_name', 'adm1_name', 'region_name')
res_stmt = sc.proj(session, num_commodities_num_markets_by_region_label, ['region_name', 'num_markets', 'num_commodities'])
#sample_res_stmt = re.random_select_k(session, res_stmt, 20)
#sample_res_latex = ex.build_tabulate(session, sample_res_stmt, 'latex')
#fh.write(sample_res_latex[1])
res_arr_stmt = re.build_array(session, res_stmt, ['num_markets', 'num_commodities', 'region_name'])
fh.close()
pltctx={'x_attr':'array_num_markets', 'y_attr':'array_num_commodities'}
pltctx['plot'] = {'title': 'distribution_subregions_per_regions',
                   'xlabel': '#subregion/regions',
                   'ylabel': '#regions',
                   'ylog' : 'False'
                   }
pltctx['graphType']= 'scatter'

vapip.plot_XY(session=session, plot_info=pltctx, stmt=res_arr_stmt)
