from Global_Food_Prices_model  import *
from population_model import *
from print_dataset_snapshot import print_dataset_snapshot
import versa_impl as vi

session = vi.init('v2')

dataset=Global_Food_Prices
attrs=['adm0_id', 'adm1_id', 'adm0_name', 'adm1_name', 'mkt_id','cm_id']
print_dataset_snapshot(session, dataset, attrs, 50, label='Global_Food_Prices')

# dataset=population
# attrs=['pid', 'age', 'gender', 'f_householdlid']
# print_dataset_snapshot(session, dataset, attrs, 15, label='population', row_width='wide')
