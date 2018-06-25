import warnings
import argparse
import logging
import cProfile
import pstats
import io
from .elastic_cvr_extract import CvrConnection, test_producer
from . import cvr_makedb
from . import setup_database_connection


def info_print(_s):
    stars = '*' * 10
    print('{0} {1} {2}'.format(stars, _s, stars))


def run_init():
    """ Create database tables and indices and fill dawa adresses """
    crdb = cvr_makedb.MakeCvrDatabase()
    crdb.create_tables()


def fill_dawa(dawa_file):
    info_print('Using File {0}'.format(dawa_file))
    crdb = cvr_makedb.MakeCvrDatabase()
    crdb.fill_dawa_table(dawa_file=dawa_file)


def fill_employment(file_path):
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
    # companies = [4007283502, 4007283515, 4007283528]
    companies = [4871908, 4001672315, 4002006883, 4001783015, 4001935148, 4002043066, 4001989398, 4000809532, 4007135401,
                 4000333615, 4001591942, 4000940152, 4000816138, 4006979367, 4006269851, 4006355987, 4001990778,
                 4295542, 5835875, 3635428, 4000869808, 4001068255, 4001657752, 4007287317, 4001727802,
                 4003830236, 4003831613, 4007287164, 4007287584, 4001662015, 4001884158, 4001504788, 4001943372,
                 4001029748, 4006694604, 4001916992, 4001417545, 4001382698, 4003802639, 4001254238, 4000817652,
                 4001664482, 4007287537, 4007250771, 4001289568, 4001401838, 4001643302, 4006601974, 4447208,
                 4007010814, 4007287221, 4001540368, 4006716034, 4001341752, 4002044009, 4000718115, 4001725888,
                 4002040300, 4001816542, 4001803865, 4001294625, 4001944478, 4000572542, 4001362162, 4000616345,
                 3565705, 4001189732, 4001630232, 4007133814, 4002037320, 4002041226, 4001490485, 3029018, 4001848008,
                 4001629588, 5519588, 4118375, 4006452381, 4001494845, 5266978, 3977902, 4002020249, 4007271374,
                 4007288084, 4003862019, 4007288064, 4915635, 4002008623, 4002041709, 5159812, 4001485982, 4006418611,
                 4006500761, 4193055, 4001984092, 4001208525, 4006493724, 4006214314, 4000723045, 5590538, 4001328045,
                 4006231731, 4001994305, 4001698795, 4001492818, 4002035540, 4000853632, 4002009640, 4002009453,
                 4000865482, 4001857202, 4001596622, 4002011366, 4002013009, 4006513864, 4001275222, 4001986155,
                 4006955671, 4001806178, 4001462608, 5893048, 5314938, 4005939397, 4000795475, 4002004159,
                 4003799780, 4000683468, 4001970642]

    # companies = [4007568555]
    # companies = [4001178549, 4001394153, 749, 4046495, 4001156549, 4001756407, 867703, 4006491007, 3595755,
    #              4006829870, 4002005199, 4001726704, 5768770, 4006916557, 4006916557, 4006511400, 4006372756,
    #              4006372744, 4006510742, 4006372756, 4006829870, 4006491007, 4056080, 4001815333, 4006395397,
    #              4001075071, 4001251542, 3214807]
    # companies = [4001575583]
    # companies = [4000981898]
    # companies = [4001582635]
    # companies = [4006898357]
    # companies = companies[0:2]
    print('insert companies')
    for comp in companies:
        print('insert', comp)
        ecvr.update_units([comp])
    # is a person
    people = [4000034553, 4004192836, 4004194126, 4000145625, 4005983489]
    # # people = [4000145625]
    print('insert people')
    ecvr.update_units(people)
    print('insert penhed')
    penhed = [4002535375, 4002241948, 4003231318]
    ecvr.update_units(penhed)


def run_delete_test(mycvr):
    companies = [4001178549, 4001394153, 749, 4046495]
    # companies = companies[0:2]
    print('delete company')
    mycvr.delete(companies, mycvr.company_type)
    people = [4000034553, 4004192836, 4004194126]
    print('delete people')
    mycvr.delete(people, mycvr.person_type)
    print('delete punit')
    penhed = [4002535375, 4002241948, 4002211395]
    mycvr.delete(penhed, mycvr.penhed_type)


def data_test(ecvr):
    enh = 743
    print('update enh', 743)
    ecvr.update_units([enh])


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
    parser.add_argument('-time', default=None, dest='time',
                        help='time test with file')
    parser.add_argument('-threading', default=False, dest='threading',
                        action='store_true')

    # logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    setup_database_connection()
    # config = db_setup.get_config()
    setup_args = {}
    # if args.disable_warnings:
    #     warnings.simplefilter("ignore")
    # if args.fill_emp:
    #     info_print('Fill exact employee numbers from file')
    #     fill_employment(dbmodel, config['employmentpath'])
    # if args.logging:
    #     logging.basicConfig(level=logging.DEBUG)
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
        logging.basicConfig(level=logging.INFO)
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
        cvr.update_units([args.enh])
    if args.time:
        pr = cProfile.Profile()
        pr.enable()
        filename = args.time
        print('testing with file', filename)
        cvr.update_from_mixed_file(filename, force=True)
        # cvr.update_from_mixed_file('/Users/jallan/tmp/cvr_update.json', force=True)
        pr.disable()
        s = io.StringIO()
        sortby = 'cumtime'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())
        print('something')
    if args.threading:
        # warnings.simplefilter("ignore")
        test_producer()
