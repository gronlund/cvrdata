import os
import warnings
import argparse
import logging

from .elastic_cvr_extract import CvrConnection
from . import cvr_makedb
from . import setup_database_connection, create_session
import cProfile, pstats, io

def info_print(s):
    stars = '*' * 10            
    print('{0} {1} {2}'.format(stars, s, stars))


def run_init():
    """ Create database tables and indices and fill dawa adresses """
    crdb = cvr_makedb.MakeCvrDatabase()
    crdb.create_tables()


def fill_dawa(dawa_file):
    info_print('Using File {0}'.format(dawa_file))
    crdb = cvr_makedb.MakeCvrDatabase()
    crdb.fill_dawa_table(dawa_file=dawa_file)


def fill_employment(db_model, file_path):
    crdb = cvr_makedb.MakeCvrDatabase()
    crdb.fill_employment_tables_from_file(file_path)


def create_views():
    """ Create database views """
    crdb = cvr_makedb.MakeCvrDatabase()
    crdb.create_views()


def run_small_test(ecvr):
    """ Small test that should put data in all tables
    
    Args:
    -----
      engine: sqlalchemy engine
    """
    # cvr = CvrConnection(engine)
    companies = [4001178549, 4001394153, 749, 4046495, 4001156549, 4001756407, 867703, 4006491007, 3595755,
                 4006829870, 4002005199, 4001726704, 5768770, 4006916557, 4006916557, 4006511400, 4006372756,
                 4006372744, 4006510742, 4006372756, 4006829870, 4006491007, 4056080, 4001815333, 4006395397,
                 4001075071, 4001251542, 3214807]
    # companies = [4001575583]
    # companies = [4000981898]
    # companies = [4001582635]
    # companies = [4006898357]
    # companies = companies[0:2]
    # print('insert company')
    ecvr.update_units(companies)
    # is a person
    people = [4000034553, 4004192836, 4004194126, 4000145625, 4005983489]
    # people = [4000145625]
    print('insert people')
    ecvr.update_units(people)
    print('insert penhed')
    penhed = [4002535375, 4002241948, 4003231318]
    ecvr.update_units(penhed)


def run_delete_test(cvr):
    companies = [4001178549, 4001394153, 749, 4046495]
    # companies = companies[0:2]
    print('delete company')
    cvr.delete(companies, cvr.company_type)
    people = [4000034553, 4004192836, 4004194126]
    print('delete people')
    cvr.delete(people, cvr.person_type)
    print('delete punit')
    penhed = [4002535375, 4002241948, 4002211395]
    cvr.delete(penhed, cvr.penhed_type)
        



if __name__ == '__main__':
    warnings.simplefilter("always")

    desc = 'Init and update cvr database'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-global', default='Global', dest='config', action='store_const', const='Global',
                        help="Use Global Database")
    parser.add_argument('-local', default='Global', dest='config', action='store_const', const='Local',
                        help="Use Local Database")
    parser.add_argument('-db', default=None, dest='db', type=str, help="Database Name")

    parser.add_argument('-init', default=False, help='Initialize database', dest='init', action='store_true')
    parser.add_argument('-create_views', default=False, help='Create Views', dest='create_views', action='store_true')
    parser.add_argument('-fill_dawa', default=False, help='Download and Fill Dawa Address Table', dest='fill_dawa',
                        action='store_true')
    parser.add_argument('-fill_emp', default=False, help='Fill Employment Table With External Data', dest='fill_emp',
                        action='store_true')
    parser.add_argument('-small_test', default=False, dest='small_test', action='store_true', help="Small Test")
    parser.add_argument('-delete_test', default=False, dest='delete_test',
                        action='store_true', help="Delete Small Test")
    parser.add_argument('-enable_address', default=False, dest='enable_add', action='store_true', help='Enable Address')
    parser.add_argument('-update', default=False, dest='update_all', action='store_true', help='Update all')
    parser.add_argument('-enh', default=None, dest='enh', help="Update Given Id", type=int)
    # parser.add_argument('-disable_warnings', default=False, dest='disable_warnings', help="Disable Warnings",
    #                     action='store_true')
    parser.add_argument('-log', default=False, dest='logging', help='enable logging', action='store_true')
    parser.add_argument('-resume', default=False, dest='resume',
                        help='resume cvr update - mainly for debugging restart', action='store_true')
    parser.add_argument('-time', default=False, dest='time',
                        help='time test', action='store_true')

    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    setup_database_connection()

    # config = db_setup.get_config()
    setup_args = {}
    # if args.disable_warnings:
    #     warnings.simplefilter("ignore")
    # if args.fill_emp:
    #     info_print('Fill exact employee numbers from file')
    #     fill_employment(dbmodel, config['employmentpath'])


    if args.logging:
        logging.basicConfig(level=logging.DEBUG)

    if args.db is not None:
        setup_args['db'] = args.db

    if args.init:
        info_print('Initialiaze database by creating necessary tables')
        run_init()
    if args.create_views:
        info_print('create views - usually best to wait to after all data has been inserted once')
        create_views()
    if args.fill_dawa:
        info_print('Download and fill dawa address tables')
        fill_dawa()
    cvr = CvrConnection(update_address=args.enable_add)
    if args.small_test:
        info_print('Running small test')
        run_small_test(cvr)
    if args.delete_test:
        info_print('Run delete test')
        run_delete_test(cvr)
    if args.update_all:
        info_print('Update All')
        cvr.update_all(args.resume)
    if args.enh is not None:
        info_print('Update specific enhedsnummer:')
        cvr.update_units(args.enh)
    if args.time:
        pr = cProfile.Profile()
        pr.enable()
        cvr.update_from_mixed_file('/Users/jallan/tmp/cvr_100k.json', force=True)
        pr.disable()
        s = io.StringIO()
        sortby = 'cumtime'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())
        print('something')