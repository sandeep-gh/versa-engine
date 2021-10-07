import versa_api as vapi
import versa_api_plot_v2 
from population_model import *
from print_dataset_snapshot import print_dataset_snapshot
session = vapi.build_session('v2')

popsz_by_age = vapi.aggregate(session, population, 'age', 'pop_sz')
res_latex=vapi.build_tabulate(session, popsz_by_age, 'latex')
print_dataset_snapshot(session, popsz_by_age, ['age', 'pop_sz'], sample_sz=10, label='popsz_by_age', row_width='narrow', randomize=False)


popsz_by_age_ordered = vapi.ascending(session, popsz_by_age, 'age')
print_dataset_snapshot(session, popsz_by_age_ordered, ['age', 'pop_sz'], sample_sz=10, label='popsz_by_age_ordered', row_width='narrow', randomize=False)

popsz_by_age_as_array= vapi.build_array(session, popsz_by_age_ordered, ['age', 'pop_sz'], 'arr_')
print_dataset_snapshot(session, popsz_by_age_as_array, ['arr_age', 'arr_pop_sz'], sample_sz=10, label='popsz_by_age_array', row_width='narrow', randomize=False)

#visualize the result
restype={'x_attr':'arr_age', 'y_attr':'arr_pop_sz'}
restype['plot'] = {'title': 'popsz_by_age_distribution',
                   'xlabel': 'age',
                   'ylabel': 'pop_sz'
                   }

vapi.plot_XY(session=session, stmt=popsz_by_age_as_array, restype=restype)




