#!/usr/bin/env python3
import argparse
import edi.dicex_edi_impl as dei
import common.utilities as utilities


parser = argparse.ArgumentParser()
###
# TODO use the work directory please
###
parser.add_argument("--port", help="specify port number",
                    nargs='?', const=None)
parser.add_argument(
    "--work_dir", help="where are all output/config/temps are written down", nargs='?')
parser.add_argument("--dbsession", nargs=1, help="dbsession identifier")
parser.add_argument("--pgdb", nargs=1,
                    help="postgres database; to be used with socialeyes")
#parser.add_argument("--metadata", nargs=1, help="the metadata file")
#parser.add_argument("--path", nargs=1, help="path to the data file")
parser.add_argument("--load_file", nargs=2,
                    help="takes two arguments; the location of the metadata and the data. ")
parser.add_argument("--edcfg", nargs=1,
                    help="external data configuration file")

parser.add_argument("--load_all", action='store_true',
                    help="load tables and files")
parser.add_argument("--load_data_oracle", action='store_true',
                    help="load tables from oracle sources")
parser.add_argument("--create_table_csv", action='store_true',
                    help="only create tables for file data; don't load any data")
parser.add_argument("--load_data_csv",  action='store_true',
                    help="create table (using metadata) and load data from the csv files")

parser.add_argument("--build_metadata_all", action='store_true',
                    help="create metadata for oracle and csv files (header is used for column names)")
parser.add_argument("--build_metadata_oracle", action='store_true',
                    help="create metadata for oracle tables only")
parser.add_argument("--build_metadata_postgres", action='store_true',
                    help="create metadata for postgres tables only")
parser.add_argument("--build_metadata_csv", action='store_true',
                    help="create metadata for csv only")
parser.add_argument("--build_metadata_shp", action='store_true',
                    help="create metadata for shapefile only")

parser.add_argument("--wrap_all", action='store_true',
                    help="create foreign data wrappers to all tables")

args = parser.parse_args()
if args.work_dir:
    dei.work_dir = args.work_dir

if args.dbsession:
    dei.init_connection(args.dbsession[0])

pgdb = 'postgres'
if args.pgdb:
    pgdb = args.pgdb[0]

# the external data config file
edcfg_xml = None

if args.edcfg:
    edcfg_xml = args.edcfg[0]
    print(edcfg_xml)

if args.load_all:
    print(args.load_all)
    dei.load_oracle_tables(edcfg_xml, commit=True)
    dei.load_postgres_tables(edcfg_xml, commit=True, make_local_copy=True)
    dei.create_tables(edcfg_xml, pgdb=pgdb)
    dei.load_files(edcfg_xml, pgdb)

if args.load_data_oracle:
    assert edcfg_xml is not None
    dei.load_oracle_tables(edcfg_xml, commit=True)

# only create the table from the metadata; -- don't load any data
if args.create_table_csv:
    assert edcfg_xml is not None
    dei.create_tables(edcfg_xml, pgdb=pgdb)

if args.load_data_csv:
    assert edcfg_xml is not None
    dei.create_tables(edcfg_xml, pgdb=pgdb)
    dei.load_files(edcfg_xml, pgdb)

if args.build_metadata_all:
    dei.build_oracle_metadata(edcfg_xml)
    dei.build_csv_metadata(edcfg_xml)
    dei.build_postgres_tables_metadata(edcfg_xml)
    dei.build_shapefile_tables_metadata(edcfg_xml)

if args.build_metadata_oracle:
    dei.build_oracle_metadata(edcfg_xml)

if args.build_metadata_postgres:
    dei.build_postgres_tables_metadata(cfg_xml)

if args.build_metadata_csv:
    dei.build_csv_metadata(edcfg_xml)

if args.build_metadata_shp:
    dei.build_shptbl_metadata(edcfg_xml)


if args.wrap_all:  # TODO: wierd option name
    dei.load_postgres_tables(edcfg_xml, commit=True, make_local_copy=False)
    dei.load_oracle_tables(edcfg_xml, commit=True, load=False)

# load a single file from (metadata, location) pair
if args.load_file:
    print(args.load_file[0])
    print(args.load_file[1])
    dei.create_table_from_metadata(args.load_file[0])
    dei.load_csv_file(args.load_file[0], args.load_file[1])
