import argparse
import pprint

from .elastic_cvr_extract import CvrConnection
from . import setup_database_connection

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download data from cvr from id')
    parser.add_argument('-global', default='Global', dest='config', action='store_const', const='Global',
                        help="Use Global Database")
    parser.add_argument('-local', default='Global', dest='config', action='store_const', const='Local',
                        help="Use Local Database")
    parser.add_argument('-db', default=None, dest='db', type=str, help="Database Name")

    parser.add_argument('-enh',
                        dest='enh',  type=int,  help="Enhedsnummer Identifier of company")
    parser.add_argument('-cvr',
                        dest='cvr',  type=int,  help="CVR Number of company")
    parser.add_argument('-pid',
                        dest='pid',  type=int,  help="pnummer of production unit")
    # parser.add_argument('-field', type=str, dest='field', help='Search field')
    # parser.add_argument('-value', type=str, dest='value', help='Search value')
    args = parser.parse_args()
    setup_database_connection()
    cvr = CvrConnection(update_address=False)

    keys = ['enh', 'cvr', 'pid']
    if args.enh is not None:
        dicts = cvr.get_entity([args.enh])
        pprint.pprint(dicts)
    if args.cvr is not None:
        dicts = cvr.get_cvrnummer(args.cvr)
        pprint.pprint(dicts)
    if args.pid  is not None:
        dicts = cvr.get_pnummer(args.pid)
        pprint.pprint(dicts)
    # if args.field is not None:
    #     dicts = cvr.search_field_val(args.field, args.value)
    #     pprint.pprint(dicts)
