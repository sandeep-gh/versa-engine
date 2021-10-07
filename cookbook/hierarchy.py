import runner
import range
import sys
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from range import IntRangeType
from intervals import IntInterval
from sqlalchemy.orm import sessionmaker
import versa_impl as vi
import versa_range_api as vra
import versa_api as vapi
import versa_utils as vu
import versa_api_join_list as vapijl

#---will change it---#
session=vi.init("geonames_rdf_data") 

Base = declarative_base()

#-----create rmo for asserted_statements table-----# 
statements = vu.import_model('kb_bec6803d52_asserted_statements')
#-----create rmo for literal_statements table-----# 
literals = vu.import_model('kb_bec6803d52_literal_statements')

#-----create alias prefix for literals rmo-----#  
literals = vapi.alias_attributes(session, literals, alias_prefix='tab2')

#-----A.PCLI means independent political entity-----#
geoname_country_label = 'http://www.geonames.org/ontology#A.PCLI'

#-----find Country URI------# 
country_uri = vapi.filterEQ(session, statements, 'object', geoname_country_label)
#----join with literals rmo to find the name of the country-----# 
#country_name_join = vapi.fuseEQ(session, country_uri, literals, 'subject', 'tab2_subject')

country_name_join = vapijl.fuseEQ(session, country_uri, literals, 'subject', 'subject', relable_common_attr=True)

#-----filter with gn:name predicates-----#
country_name_filter = vapi.filterEQ(session, country_name_join, 'tab2_predicate', 'http://www.geonames.org/ontology#name')
#-----find country name (literal value)-----#
country_name = vapi.proj(session, country_name_filter, 'tab2_object')
#-----scan country name to print-----#
country_result = vapi.scan(session, country_name)

#-----create an alias prefix for statements rmo-----# 
statements_alias_1 = vapi.alias_attributes(session, statements, alias_prefix='tab11')

#-----self join with statements rmo, goal is to find region URI-----#
region_join = vapi.fuseEQ(session, country_uri, statements_alias_1, 'subject', 'tab11_subject')
#-----filter with gn:parentFeature, goal is to find country's region parent feature----#  
region_uri_filter = vapi.filterEQ(session, region_join, 'tab11_predicate', 'http://www.geonames.org/ontology#parentFeature')

#-----create a second alias prefix for statements rmo-----#
statements_alias_2 = vapi.alias_attributes(session, statements, alias_prefix='tab12')

#-----self join with statements rmo, goal is to find continent URI-----#
continent_join = vapi.fuseEQ(session, region_uri_filter, statements_alias_2, 'tab11_object', 'tab12_subject')
#-----filter with gn:parentFeature, goal is to find region's  continent parent feature----#  
continent_uri_filter = vapi.filterEQ(session, continent_join, 'tab12_predicate', 'http://www.geonames.org/ontology#parentFeature')
#-----join with literals rmo to find continent name-----#
continent_name_join = vapi.fuseEQ(session, continent_uri_filter, literals, 'tab12_object', 'tab2_subject')
#-----filter with gn:name predicates-----#
continent_name_filter = vapi.filterEQ(session, continent_name_join, 'tab2_predicate', 'http://www.geonames.org/ontology#name')
#-----find continent name (literal value)-----#
continent_name = vapi.proj(session, continent_name_filter, 'tab2_object')
#-----scan continent name to print-----#
continent_result = vapi.scan(session, continent_name)
 
#-----print country and continent names-----#
for (country, continent) in zip(country_result, continent_result):
        print "Country:", country, "Continent:", continent



#-----admin1-----#
#admin1 = vapi.filterEQ(session, statements, 'object', 'http://www.geonames.org/ontology#A.ADM1')

#admin1_join = vapi.fuseEQ(session, admin1, literals, 'subject', 'tab2_subject')
#admin1_filter = vapi.filterEQ(session, admin1_join, 'tab2_predicate', 'http://www.geonames.org/ontology#name')



#a = vapi.alias_attributes(session, a, alias_prefix='a')
#z = vapi.fuseEQ(session, b, a, 'subject', 'a_subject')
#y = vapi.filterEQ(session, z, 'subject', 'http://sws.geonames.org/2274890/')  
#x = vapi.limit(session, y, 1) 
#result = vapi.scan(session, region_uri_filter)

#for i in result:
#	print i

