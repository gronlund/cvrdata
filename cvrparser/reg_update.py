import warnings
import argparse
import logging
from .elastic_reg_extract import RegistrationConnection
from . import setup_database_connection


def info_print(_s):
    stars = '*' * 10
    print('{0} {1} {2}'.format(stars, _s, stars))


def run_small_test(regconn):
    """ Small test that should put data in all tables
    
    Args:
    -----
      engine: sqlalchemy engine
    """
    companies = [35865497, 31588073, 35225358, 26185777, 14132678, 18373432, 14130705]
    print('insert company updates')
    for comp in companies:
        print('insert', comp)
        regs =  regconn.get_cvrnummer(comp)
        regs = [x['_source'] for x in regs]
        regconn.insert_registrations([regs])


if __name__ == '__main__':
    warnings.simplefilter("always")

    desc = 'Init and update cvr database'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-global', default='Global', dest='config', action='store_const', const='Global',
                        help="Use Global Database")
    parser.add_argument('-local', default='Global', dest='config', action='store_const', const='Local',
                        help="Use Local Database")
    parser.add_argument('-db', default=None, dest='db', type=str, help="Database Name")

    parser.add_argument('-small_test', default=False, dest='small_test', action='store_true', help="Small Test")

    args = parser.parse_args()
    setup_database_connection()
    if args.db is not None:
        setup_args['db'] = args.db
    regconn = RegistrationConnection()
    if args.small_test:
        logging.basicConfig(level=logging.INFO)
        info_print('Running small test')
        run_small_test(regconn)
