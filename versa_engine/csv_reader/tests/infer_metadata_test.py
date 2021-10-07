import csv_infer_schema as cis
import sys

#file_path = "/media/kabira/home/databank/versa/data/maps-master/docs/data/csv/statesCensus.csv"
file_path = "/media/kabira/home/sandeep_ndssl_desktop_home/Downloads/2014_Adult_HIV_prevalence_rate_by_County.csv"
file_path = "/media/kabira/home/sandeep/ndsslgit/national_flu/modules/travel/commuting/countypops_2013.csv"
file_path = "/media/kabira/home/sandeep/myprojects/Census_for_global_population_construction/all_census.csv"
metadata_path = "/tmp/test_model.md"


if len(sys.argv) > 1:
    cmr = cis.get_csv_report(sys.argv[1])
else:
    cmr = cis.get_csv_report(file_path)


print(cmr)
