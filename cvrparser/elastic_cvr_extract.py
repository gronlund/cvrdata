from elasticsearch1 import Elasticsearch
from elasticsearch1_dsl import Search
from collections import defaultdict, namedtuple
import datetime
import ujson as json
import os
import pytz
import tqdm
import logging
import threading
from .field_parser import utc_transform
from . import config, create_session
from . import alchemy_tables
from .bug_report import add_error
from . import data_scanner
from .cvr_download import download_all_dicts_to_file

# from multiprocessing import Process, Queue, Lock
#
#
# # Producer function that places data on the Queue
# def producer(queue, lock, names):
#     # Synchronize access to the console
#     with lock:
#         print('Starting producer => {}'.format(os.getpid()))
#
#     # Place our names on the Queue
#     for name in names:
#         queue.put(name)
#
#     # Synchronize access to the console
#     with lock:
#         print('Producer {} exiting...'.format(os.getpid()))
#
#
# # The consumer function takes data off of the Queue
# def consumer(queue, lock):
#     # Synchronize access to the console
#     with lock:
#         print('Starting consumer => {}'.format(os.getpid()))
#
#     # Run indefinitely
#     while True:
#
#         # If the queue is empty, queue.get() will block until the queue has data
#         name = queue.get()
#
#         # Synchronize access to the console
#         with lock:
#             print('{} got {}'.format(os.getpid(), name))
#

class CvrConnection(object):
    """ Class for connecting and retrieving data from danish CVR register """
    def __init__(self, update_address=False):
        """ Setup everything needed for elasticsearch
        connection to Danish Business Authority for CVR data extraction
        consider moving elastic search connection into __init__
        currently inserts 300-400 units per second to database.
        So insert of all will take around 5-6 hours.

        Add treading/parallelism so we insert while we scan for updates.
        Seems downloading is really slow.



        Args:
        -----
          :param update_address: bool,
        determine if parse and insert address as well (slows it down)
        """
        self.url = 'http://distribution.virk.dk:80'
        self.index = 'cvr-permanent'
        self.company_type = 'virksomhed'
        self.penhed_type = 'produktionsenhed'
        self.person_type = 'deltager'
        user = config['cvr_user']
        password = config['cvr_passwd']
        self.datapath = config['data_path']
        self.update_batch_size = 2048
        self.source_keymap = {'virksomhed': 'Vrvirksomhed',
                              'deltager': 'Vrdeltagerperson',
                              'produktionsenhed': 'VrproduktionsEnhed'}
        self.update_address = update_address
        self.address_parser_factory = data_scanner.AddressParserFactory()
        self.elastic_client = Elasticsearch(self.url,
                                            http_auth=(user, password),
                                            timeout=60,
                                            max_retries=10,
                                            retry_on_timeout=True)
        self.elastic_search_scan_size = 128
        self.elastic_search_scroll_time = u'10m'
        # max number of updates to download without scan scroll
        self.max_download_size = 200000
        self.update_info = namedtuple('update_info',
                                      ['samtid', 'sidstopdateret'])
        self.update_list = namedtuple('update_list',
                                      ['enhedsnummer', 'sidstopdateret'])
        self.dummy_date = datetime.datetime(year=1001,
                                            month=1,
                                            day=1,
                                            tzinfo=pytz.utc)
        self.data_file = os.path.join(self.datapath, 'cvr_all.json')

    def search_field_val(self, field, value, size=10):
        search = Search(using=self.elastic_client, index=self.index)
        search = search.query('match', **{field: value}).extra(size=size)
        logging.info('field value search {0}'.format(search.to_dict()))
        response = search.execute()
        hits = response.hits.hits
        return hits

    def get_entity(self, enh):
        """ Get CVR info from given entities
        
        Args:
        -----
         :param enh: list, list of CVR ids (enhedsnummer)
        """
        search = Search(using=self.elastic_client, index=self.index)
        search = search.query('ids', values=enh).extra(size=len(enh))
        # search = search.query('match', values=enh)
        logging.info('enhedsnummer search in cvr: {0}'.format(search.to_dict()))
        response = search.execute()
        hits = response.hits.hits
        return hits

    def get_pnummer(self, pnummer):
        """ Get CVR info from given production unit id

        Args:
        -----
          pnummer: id of production unit
        """
        search = Search(using=self.elastic_client, index=self.index)
        search = search.query('match', _type=self.penhed_type)
        search = search.query('match', **{'VrproduktionsEnhed.pNummer': pnummer})
        logging.info(('pnummer search: '.format(search.to_dict())))
        response = search.execute()
        hits = response.hits.hits
        return hits

    def get_cvrnummer(self, cvrnummer):
        """ Get CVR info from given cvr id

        :param cvrnummer: int, cvrnumber of company
        :return: dict, data for company
        """
        search = Search(using=self.elastic_client, index=self.index)
        search = search.query('match', **{'Vrvirksomhed.cvrNummer': cvrnummer})
        logging.info('cvr id search: {0}'.format(search.to_dict()))
        response = search.execute()
        hits = response.hits.hits
        return hits

    def update_all(self, resume_cvr_all=False):
        """
        Update CVR Company Data
        download updates
        perform updates

        rewrite to producer consumer.
        """
        session = create_session()
        ud_table = alchemy_tables.Update
        res = session.query(ud_table).first()
        session.close()
        old_file_used = False
        if res is None or resume_cvr_all:
            filename = self.data_file
            if os.path.exists(filename):
                print('Old file {0} found - using it'.format(filename))
                old_file_used = True
            else:
                self.download_all_dicts(filename)
        else:
            update_info = self.get_update_list()
            filename = self.download_all_dicts_from_update_info(update_info)
        print('Start Updating Database')
        self.update_from_mixed_file(filename)
        if old_file_used and not resume_cvr_all:
            print('Inserted the old files - now update to newest version, i will call myself')
            self.update_all()

    def download_all_dicts(self, filename):
        """
        :return:
        str: filename, datetime: download time, bool: new download or use old file
        """
        params = {'scroll': self.elastic_search_scroll_time, 'size': self.elastic_search_scan_size}
        search = Search(using=self.elastic_client, index=self.index)
        search = search.query('match_all')
        search = search.params(**params)
        download_all_dicts_to_file(filename, search)

    def download_all_dicts_from_update_info(self, update_info):
        """
        :param update_info:  update_info, dict with update info for each unit type
        :return:
        str: filename, datetime: download time, bool: new download or use old file
        """
        print('Download Data Write to File')
        filename = os.path.join(self.datapath, 'cvr_update.json')
        if os.path.exists(filename):
            "filename exists {0} overwriting".format(filename)
            os.remove(filename)
        print('Download updates to file name: {0}'.format(filename))
        params = {'scroll': self.elastic_search_scroll_time, 'size': self.elastic_search_scan_size}
        for (cvr_type, _type) in self.source_keymap.items():
            print('Downloading Type {0}'.format(_type))
            search = Search(using=self.elastic_client, index=self.index)
            if len(update_info[_type]['units']) < self.max_download_size:
                print('Few to update: {0}\nGet in match query:'.format(len(update_info[_type]['units'])))
                units = [x[0] for x in update_info[_type]['units']]
                search = search.query('ids', values=units)
            else:
                print('To many for match query... - Get a lot of stuff we do not need')
                search = search.query('range',
                                      **{'{0}.sidstOpdateret'.format(_type):
                                      {'gte': update_info[_type]['sidstopdateret']}})
                # search = search.query('match', _type=cvr_type)
                # search = search.query('match',

            search = search.params(**params)
            download_all_dicts_to_file(filename, search, mode='a')
            print('{0} handled:'.format(_type))
        return filename

    def update_units(self, enh):
        """ Force download and update of given units

        Args:
        -----
          enh: list , id of units to update (enhedsnummer)
        """
        data_list = self.get_entity(enh)
        dicts = {x: list() for x in self.source_keymap.values()}
        for data_dict in data_list:
            dict_type = data_dict['_type']
            key = self.source_keymap[dict_type]
            dicts[key].append(data_dict['_source'][key])
            if len(dicts[key]) >= self.update_batch_size:
                self.update(dicts[key], key)
                dicts[key].clear()
        for enh_type, _dicts in dicts.items():
            if len(_dicts) > 0:
                self.update(_dicts, enh_type)

    def update(self, dicts, _type):
        """ Update given entities

        Args:
            dicts: list of dictionaries with data
            _type: string, type object to update
        """
        enh = [x['enhedsNummer'] for x in dicts]
        self.delete(enh, _type)
        try:
            self.insert(dicts, _type)
        except Exception as e:
            print(e)
            print('enh failed', enh)
            raise e
        # print('Update Done!')

    @staticmethod
    def delete(enh, _type):
        """ Delete data from given entities
        
        Args:
        -----
        enh: list of company ids (enhedsnummer)
        _type: object type to delete
        """
        # logging.info('Start delete')
        delete_table_models = [alchemy_tables.Update,
                               alchemy_tables.Adresseupdate,
                               alchemy_tables.Attributter,
                               alchemy_tables.Livsforloeb,
                               alchemy_tables.AarsbeskaeftigelseInterval,
                               alchemy_tables.KvartalsbeskaeftigelseInterval,
                               alchemy_tables.MaanedsbeskaeftigelseInterval,
                               alchemy_tables.SpaltningFusion]
        if _type == 'Vrvirksomhed':
            static_table = alchemy_tables.Virksomhed
        elif _type == 'VrproduktionsEnhed':
            static_table = alchemy_tables.Produktion
        elif _type == 'Vrdeltagerperson':
            static_table = alchemy_tables.Person
        else:
            print('bad _type: ', _type)
            raise Exception('bad _type')
        delete_table_models.append(static_table)
        #  delete independently from several tables. Lets thread them
        def worker(i):
            session = create_session()
            table_class = delete_table_models[i]
            # print('delete', table_class)
            session.query(table_class).filter(table_class.enhedsnummer.in_(enh)).delete(synchronize_session=False)
            session.commit()
            session.close()

        def enh_worker():
            session = create_session()
            session.query(alchemy_tables.Enhedsrelation). \
                filter(alchemy_tables.Enhedsrelation.enhedsnummer_virksomhed.in_(enh)). \
                delete(synchronize_session=False)
            session.commit()
            session.close()

        threads = []
        if _type == 'Vrvirksomhed':
            t = threading.Thread(target=enh_worker)
            threads.append(t)
            t.start()
        for i in range(len(delete_table_models)):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # session = create_session()
        # try:
        #     for table_model in delete_table_models:
        #         session.query(table_model).filter(table_model.enhedsnummer.in_(enh)).delete(synchronize_session=False)
        #     if _type == 'Vrvirksomhed':
        #         session.query(alchemy_tables.Enhedsrelation).\
        #             filter(alchemy_tables.Enhedsrelation.enhedsnummer_virksomhed.in_(enh)).\
        #             delete(synchronize_session=False)
        #     session.commit()
        # except Exception as e:
        #     print('Delete Exception:', enh)
        #     print(e)
        #     session.rollback()
        #     session.close()
        #     raise
        # session.close()

    def insert(self, dicts, enh_type):
        """ Insert data from dicts

        Args:
        :param dicts: list of dicts with cvr data (Danish Business Authority)
        :param enh_type: cvr object type
        """
        # logging.info('start insert')
        data_parser = data_scanner.DataParser(_type=enh_type)
        address_parser = self.address_parser_factory.create_parser(self.update_address)
        data_parser.parse_data(dicts)
        # logging.info('fixed valued parsed')
        # print('value data inserted - start dynamic ')
        data_parser.parse_dynamic_data(dicts)
        # print('dynamic data inserted')
        address_parser.parse_address_data(dicts)
        # print('address data inserted/skipped - start static')
        data_parser.parse_static_data(dicts)
        # print('static parsed')

    def make_samtid_table(self):
        """ Make mapping from entity id to current version
        Add threading to run in parallel to see if that increase speed.
        """
        print('Make id -> samtId map: units update status map')
        table_models = [alchemy_tables.Virksomhed, alchemy_tables.Produktion, alchemy_tables.Person]
        enh_samtid_map = defaultdict()
        session = create_session()
        for table in table_models:
            query = session.query(table.enhedsnummer,
                                  table.samtid,
                                  table.sidstopdateret)
            existing_data = [(x[0], x[1], x[2]) for x in query.all()]
            tmp = {a: self.update_info(samtid=b, sidstopdateret=c) for (a, b, c) in existing_data}
            enh_samtid_map.update(tmp)
        session.close()
        print('Id map done')
        return enh_samtid_map

    def update_from_mixed_file(self, filename, force=False):
        """ splits data in file by type and updates the database

        :param filename: str, filename full path
        :param force: bool, force to update all
        :return:
        """
        print('Start Reading From File', filename)
        if force:
            enh_samtid_map = {}
        else:
            enh_samtid_map = self.make_samtid_table()
        dummy = self.update_info(samtid=-1, sidstopdateret=self.dummy_date)
        dicts = {x: list() for x in self.source_keymap.values()}
        with open(filename) as f:
            for line in tqdm.tqdm(f):
                raw_dat = json.loads(line)
                keys = raw_dat.keys()
                dict_type_set = keys & self.source_keymap.values()  # intersects the two key sets
                if len(dict_type_set) != 1:
                    add_error('BAD DICT DOWNLOADED', raw_dat)
                    continue
                dict_type = dict_type_set.pop()
                dat = raw_dat[dict_type]
                enhedsnummer = dat['enhedsNummer']
                samtid = dat['samtId']
                if dat['samtId'] is None:
                    add_error('Samtid none.', enhedsnummer)
                    dat['samtId'] = -1
                    samtid = -1
                current_update = enh_samtid_map[enhedsnummer] if enhedsnummer in enh_samtid_map else dummy
                if samtid > current_update.samtid:
                    # update if new version - currently or sidstopdateret > current_update.sidstopdateret:
                    dicts[dict_type].append(dat)
                if len(dicts[dict_type]) >= self.update_batch_size:
                    self.update(dicts[dict_type], dict_type)
                    dicts[dict_type].clear()
        for enh_type, _dicts in dicts.items():
            if len(_dicts) > 0:
                self.update(_dicts, enh_type)
        print('file read all updated')

    def get_update_list(self):
        """ Find units that needs updating and their sidstopdateret (last updated)
        the sidstopdateret may be inaccurate and thus way to far back in time therefore we cannot use take the largest
        of sidstopdateret from the database. Seems we download like 600 dicts a second with match_all.
        Should take around 2 hours and 30 minuttes then. This takes 30 so i need to save half an hour on downloads.

        :return datetime (min sidstopdateret), list (enhedsnumer, sidstopdateret)
        """
        enh_samtid_map = self.make_samtid_table()
        oldest_sidstopdateret = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)+datetime.timedelta(days=1)
        update_dicts = {x: {'units': [], 'sidstopdateret': oldest_sidstopdateret} for x in self.source_keymap.values()}
        if len(enh_samtid_map) == 0:
            return update_dicts
        dummy = self.update_info(samtid=-1, sidstopdateret=self.dummy_date)
        print('Get update time for all data')

        for _type in self.source_keymap.values():
            search = Search(using=self.elastic_client, index=self.index)
            search = search.query('match_all')
            sidst_key = '{0}.sidstOpdateret'.format(_type)
            samt_key = '{0}.samtId'.format(_type)
            field_list = ['_id', sidst_key, samt_key]
            # field_list = ['_id'] + ['{0}.sidstOpdateret'.format(key) for key in self.source_keymap.values()] + \
            #          ['{0}.samtId'.format(key) for key in self.source_keymap.values()]
            search = search.fields(fields=field_list)
            params = {'scroll': self.elastic_search_scroll_time, 'size': 2**11}
            search = search.params(**params)
            print('ElasticSearch Query: ', search.to_dict())
            generator = search.scan()
            for cvr_update in tqdm.tqdm(generator):
                enhedsnummer = int(cvr_update.meta.id)
                raw_dat = cvr_update.to_dict()
                samtid = raw_dat[samt_key][0] if samt_key in raw_dat else None
                sidstopdateret = raw_dat[sidst_key][0] if sidst_key in raw_dat else None
                if sidstopdateret is None or samtid is None:
                    continue
                current_update = enh_samtid_map[enhedsnummer] if enhedsnummer in enh_samtid_map else dummy
                if samtid > current_update.samtid:
                    utc_sidstopdateret = utc_transform(sidstopdateret)
                    update_dicts[_type]['sidstopdateret'] = min(utc_sidstopdateret,
                                                                update_dicts[_type]['sidstopdateret'])
                    update_dicts[_type]['units'].append((enhedsnummer, utc_sidstopdateret))
                    # break
        print('Update Info: ')
        print([(k, v['sidstopdateret'], len(v['units'])) for k, v in update_dicts.items()])
        return update_dicts

    def optimize_download_updated(self, update_info):
        """ Due to a missing sidstopdateret for employment updates in cvr
        the sidstopdateret may be inaccurate and thus way to far back in time
        Update the self.max_download_size oldest to see if that helps us use a reasonable sidstopdateret data
        maybe it is actually the punits that gives the biggest issue here.

        :param update_info:
        :return:
        """
        for _type, info in update_info.items():
            units = info['units']
            if len(units) < self.max_download_size:
                enh = [x[0] for x in units]
                self.update_units(enh)
                info['units'] = []
                info['sidstopdateret'] = None
            else:
                sorted(units, key=lambda x: x[1])
                first_enh = [x[0] for x in units[0:self.max_download_size]]
                new_sidst_opdateret = units[self.max_download_size][1]
                self.update_units(first_enh)
                info['units'] = units[self.max_download_size:]
                info['sidstopdateret'] = new_sidst_opdateret
        return update_info

    def find_missing(self):
        """
        Check if we are missing anything

        :return:
        """
        search = Search(using=self.elastic_client, index=self.index)
        search = search.query('match_all')
        field_list = ['_id']
        search = search.fields(fields=field_list)
        params = {'scroll': self.elastic_search_scroll_time, 'size': 2*self.elastic_search_scan_size}
        search = search.params(**params)
        print('ElasticSearch Query: ', search.to_dict())
        generator = search.scan()
        ids = [x.meta.id for x in generator]
        return ids
