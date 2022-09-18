import argparse
import pprint
from . import (interactive_ensure_config_exists,
               setup_database_connection,
               interactive_configure_connection,
               read_config)
from .elastic_cvr_extract import CvrConnection
from .elastic_reg_extract import RegistrationConnection
from . import cvr_makedb

class Commands:

    @staticmethod
    def dbsetup(create_tables, create_query_indexes, create_views, drop_views):
        interactive_ensure_config_exists()
        setup_database_connection()
        crdb = cvr_makedb.MakeCvrDatabase()
        if create_tables:
            crdb.create_tables()
        if create_views:
            crdb.create_views()
        if create_query_indexes:
            crdb.create_query_indexes()
        if not (create_query_indexes or create_views or create_tables):
            print('No command option given.')
            print('-t create_tables\n-v create views\n-i create indicesx')

    @staticmethod
    def get_regs():
        interactive_ensure_config_exists()
        setup_database_connection()
        RegistrationConnection.insert_all()
        #regconn.get_all()
        
    
    @staticmethod
    def update(use_address, resume, num_workers):
        interactive_ensure_config_exists()
        setup_database_connection()
        cvr = CvrConnection(update_address=use_address)
        cvr.update_all(resume, num_workers)

    @staticmethod
    def query(enh, cvr, pid, **general_options):
        interactive_ensure_config_exists()
        setup_database_connection()
        cvr_conn = CvrConnection(update_address=False)
        print('options', enh, cvr, pid)
        if enh is not None:
            dicts = cvr_conn.get_entity([enh])
            pprint.pprint(dicts)
        if cvr is not None:
            dicts = cvr_conn.get_cvrnummer(cvr)
            pprint.pprint(dicts)
        if pid is not None:
            dicts = cvr_conn.get_pnummer(pid)
            pprint.pprint(dicts)

    @staticmethod
    def dawa(**general_options):
        interactive_ensure_config_exists()
        setup_database_connection()
        crdb = cvr_makedb.MakeCvrDatabase()
        crdb.insert_dawa()

    @staticmethod
    def reconfigure(**general_options):
        interactive_configure_connection()

    @staticmethod
    def showconfig(**general_options):
        config = read_config()['Global']
        for key, value in config.items():
            print(key, value)


parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(dest='command')
subparsers.required = True

reg_update = subparsers.add_parser('get_regs', help='get data from registration database')
parser_update = subparsers.add_parser('update', help='update data from erst')

parser_update.add_argument('-a', '--use_address',
                           dest='use_address',
                           help='Enable Address Parsing - Requires Dawa download first - and is slower',
                           default=False,
                           action='store_true',
                           )
parser_update.add_argument('-r', '--resume',
                           dest='resume',
                           help='Resume parsing first file downloaded from erst.',
                           default=False,
                           action='store_true'
                           )
parser_update.add_argument('-n', '--num_workers',
                           dest='num_workers',
                           help='speficfy number of workers.',
                           default=3,
                           type=int
                           )


parser_dawa = subparsers.add_parser('dawa',
                                    help='Download dawa address info and insert into sql database')
parser_query = subparsers.add_parser('query',
                                     help='query the erst service for a particular unit')
parser_query.add_argument('-e', '--enh', dest='enh',
                          type=int, help="Enhedsnummer Identifier of company")
parser_query.add_argument('-c', '--cvr', dest='cvr',
                          type=int, help="CVR Number of company")
parser_query.add_argument('-p', '--pid', dest='pid',
                          type=int, help="pnummer of production unit")

parser_reconfigure = subparsers.add_parser('reconfigure',
                                           help='Reconfigure configuration.')

parser_showconfig = subparsers.add_parser('showconfig',
                                          help='Show Configuration')

parser_setup = subparsers.add_parser('dbsetup',
                                     help='Setup data base tables, views, and indexes')
parser_setup.add_argument('-t', '--tables',
                          dest='create_tables',
                          help='Build tables before update. Build query index and views after first update',
                          default=False,
                          action='store_true',
                          )
parser_setup.add_argument('-v', '--views',
                          dest='create_views',
                          help='Build useful data views',
                          default=False,
                          action='store_true'
                          )
parser_setup.add_argument('-i', '--indexes',
                          dest='create_query_indexes',
                          help='Build useful query indexes. Build after first update',
                          default=False,
                          action='store_true'
                          )
parser_setup.add_argument('-dv_force', '--drop_views',
                          dest='drop_views',
                          help='Drop Views',
                          default=False,
                          action='store_true'
                          )
parser_setup.add_argument('-dt_force', '--drop_tables',
                          dest='drop_views',
                          help='Drop Views',
                          default=False,
                          action='store_true'
                          )



if __name__ == "__main__":
    args = vars(parser.parse_args())
    getattr(Commands, args.pop('command'))(**args)
