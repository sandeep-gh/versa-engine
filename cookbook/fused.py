import versa_impl as vi
from rdflib import Graph
from rdflib import BNode
from rdflib import Namespace
from rdflib import URIRef
from rdflib.namespace import RDF
import rdflib
from rdflib.plugins.sparql import prepareQuery
#from rdflib_sqlalchemy.SQLAlchemy import SQLAlchemy
from rdflib_sqlalchemy import store
import rdflib_sqlalchemy.store as store 
#rdflib_sqlalchemy.store.SQLAlchemy
import pprint
import logging

def hierarchy():
	
	#session = vi.init('geonames')
	#engine = session.connection().engine		
	#------------Open a Store------------#
	mystore = store.SQLAlchemy(configuration="postgresql://localhost:5795/postgres")
	#mystore = store.SQLAlchemy(engine=engine)
	#----------Open RDF Graph------------#
	g = Graph(mystore, identifier="geonamesNTFile")
	r = g.parse("geonames.nt", format="nt")
	g.store
	
	#---------- RDF Graph Length--------#
	#count = len(g)
	#print count
	
	#-----------Test Query--------------#
	#qres = g.query(
	#	"""
	#	SELECT * WHERE {?s ?p ?o} LIMIT 1
	#	""")

	#for s, p, o in qres:
	#	print s		
			

if __name__== '__main__':
	hierarchy()
