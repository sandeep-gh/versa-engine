import sqlalchemy
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table,Column, Integer, String, Table, Interval
import sqlite3
from sqlalchemy.orm import aliased
from sqlalchemy import Column, ForeignKey, Integer, Interval, String, create_engine
from sqlalchemy.orm import Session, relationship, backref,\
                                joinedload
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.collections import attribute_mapped_collection

#import scipy.stats as stats
import matplotlib
matplotlib.use('pdf')
import matplotlib.pyplot as plt
import numpy as np


import sys
import os

#from versa_api import *
#from versa_apip import * #loades seaborn which seems to be running into trouble

module_dir=os.path.dirname(os.path.realpath(__file__))
port=None
conn_str = None
engine = None
Session = None
session=None
work_dir=None
