from elasticsearch import Elasticsearch
# import elasticsearch_dsl
from elasticsearch_dsl import Search
from collections import namedtuple
import datetime
import ujson as json
import os
import pytz
import tqdm
import logging
import threading
from .field_parser import utc_transform
from . import config, create_session, engine, setup_database_connection
from . import alchemy_tables
from .bug_report import add_error
from . import data_scanner
from .cvr_download import download_all_dicts_to_file
from multiprocessing.pool import Pool
import multiprocessing
import time
import sys


def update_all_mp(workers=1):
    # https://docs.python.org/3/howto/logging-cookbook.html
    lock = multiprocessing.Lock()
    queue_size = 30000
    queue = multiprocessing.Queue(maxsize=queue_size)  # maxsize=1000*1000*20)
    prod = multiprocessing.Process(target=cvr_update_producer, args=(queue, lock))
    # prod.daemon = True
    prod.start()
    consumers = [multiprocessing.Process(target=cvr_update_consumer, args=(queue, lock))
                 for _ in range(workers)]
    for c in consumers:
        c.daemon = True
        c.start()
    try:
        prod.join()
        print('Producer done', 'adding sentinels')
        with lock:
            print('Producer Done - Adding Sentinels')
    except Exception as e:
        with lock:
            print('Something wroing in waiting for producer')
            print('Exception:', e)
    for i in range(workers):
        print('Adding sentinel', i)
        queue.put(CvrConnection.cvr_sentinel)
    
    for c in consumers:
        print('waiting for consumers', c)
        c.join()
    print('all consumers done')
    queue.close()
    

def create_elastic_connection(url, authentication, timeout=60, max_retries=10, retry=True):
    return Elasticsearch(url,
                         http_auth=authentication,
                         timeout=timeout,
                         max_retries=max_retries,
                         retry_on_timeout=retry,
                         http_compress=True)


class CvrConnection(object):
    """ Class for connecting and retrieving data from danish CVR register """
    dummy_date = datetime.datetime(year=1001, month=1, day=1, tzinfo=pytz.utc)
    source_keymap = {'virksomhed': 'Vrvirksomhed',
                     'deltager': 'Vrdeltagerperson',
                     'produktionsenhed': 'VrproduktionsEnhed'}
    update_info = namedtuple('update_info', ['samtid', 'sidstopdateret'])
    cvr_sentinel = 'CVR_SENTINEL'
    cvr_nothing = 'NOTHING_RETURNED'

    def __init__(self, update_address=False):
        """ Setup everything needed for elasticsearch
        connection to Danish Business Authority for CVR data extraction
        consider moving elastic search connection into __init__

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
        self.user = config['cvr_user']
        self.password = config['cvr_passwd']
        # self.datapath = config['data_path']
        self.update_batch_size = 64
        self.update_address = update_address
        self.address_parser_factory = data_scanner.AddressParserFactory()
        # self.ElasticParams = [self.url, (self.user, self.password), 60, 10, True]
        self.elastic_client = create_elastic_connection(self.url, (self.user, self.password))
        print('Elastic Search Client:', self.elastic_client.info())
        self.elastic_search_scan_size = 128
        self.elastic_search_scroll_time = u'20m'
        # max number of updates to download without scan scroll
        self.max_download_size = 200000
        self.update_list = namedtuple('update_list',
                                      ['enhedsnummer', 'sidstopdateret'])
        self.dummy_date = datetime.datetime(year=1001,
                                            month=1,
                                            day=1,
                                            tzinfo=pytz.utc)
        # self.data_file = os.path.join(self.datapath, 'cvr_all.json')

    def search_field_val(self, field, value, size=10):
        search = Search(using=self.elastic_client, index=self.index)
        search = search.query('match', **{field: value}).extra(size=size)
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
        response = search.execute()
        hits = response.hits.hits
        return hits

    @staticmethod
    def update_all(self, worker_count=3):
        """
        Update CVR Company Data
        download updates
        perform updates

        rewrite to producer consumer.
        """
        update_all_mp(worker_count)
        return
        # assert False, 'DEPRECATED'
        # session = create_session()
        # ud_table = alchemy_tables.Update
        # res = session.query(ud_table).first()
        # session.close()
        # if res is None:
        #    update_all_mp(3)
        # else:
        #     update_since_last(3)

    def download_all_data_to_file(self, filename):
        """
        :return:
        str: filename, datetime: download time, bool: new download or use old file
        """
        params = {'scroll': self.elastic_search_scroll_time, 'size': self.elastic_search_scan_size}
        search = Search(using=self.elastic_client, index=self.index)
        search = search.query('match_all')
        search = search.params(**params)
        download_all_dicts_to_file(filename, search)

    def download_all_dicts_to_file_from_update_info(self, update_info):
        """ DEPRECATED
        :param update_info:  update_info, dict with update info for each unit type
        :return:
        str: filename, datetime: download time, bool: new download or use old file
        """
        print('Download Data Write to File - DEPRECATED')
        filename = os.path.join('/data/cvr_update.json')
        if os.path.exists(filename):
            "filename exists {0} overwriting".format(filename)
            os.remove(filename)
        print('Download updates to file name: {0}'.format(filename))
        params = {'scroll': self.elastic_search_scroll_time, 'size': self.elastic_search_scan_size}
        for (_, _type) in self.source_keymap.items():
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
        for _data_dict in data_list:
            # dict_type = data_dict['_type']
            # print('data dict', data_dict)
            #dict_type = data_dict['_type']
            try:
                data_dict = _data_dict.to_dict()
            except Exception as e:
                data_dict = _data_dict
            
            keys = data_dict['_source'].keys()
            dict_type_set = keys & CvrConnection.source_keymap.values()  # intersects the two key sets
            if len(dict_type_set) != 1:
                #import pdb
                #pdb.set_trace()
                add_error('BAD DICT DOWNLOADED {0} - {1}'.format(data_dict, _data_dict))
                continue
            key = dict_type_set.pop()
            # if dict_type not in self.source_keymap:
            #     dict_type = data_dict['_source'].keys()
            #     import pdb
            #     pdb.set_trace()
            #     if data_dict['_source']['enhedsType'] == 'VIRKSOMHED':
            #         dicts['Vrvirksomhed'].append(data_dict['_source'])
            #     else:
            #         import pdb
            #         pdb.set_trace()
            #         assert False
            # key = self.source_keymap[dict_type]
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
        CvrConnection.delete(enh, _type)
        try:
            self.insert(dicts, _type)
        except Exception as e:
            print(e)
            print('enh {0} failed - enh_type: {1}'.format(enh, _type))
            raise e
        # print('Update Done!')

    def update_employment_only(self, dicts, _type):
        """ Update employment - used due to cvr bug that does not update version id when updating employment so we just always update that

        Args:
            dicts: list of dictionaries with data
            _type: string, type object to update
        """
        enh = [x['enhedsNummer'] for x in dicts]
        CvrConnection.delete_employment_only(enh)
        try:
            self.insert_employment_only(dicts, _type)
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
        delete_table_models = [alchemy_tables.Update,
                               alchemy_tables.Adresseupdate,
                               alchemy_tables.Attributter,
                               alchemy_tables.Livsforloeb,
                               alchemy_tables.Aarsbeskaeftigelse,
                               alchemy_tables.Kvartalsbeskaeftigelse,
                               alchemy_tables.Maanedsbeskaeftigelse,
                               alchemy_tables.erstAarsbeskaeftigelse,
                               alchemy_tables.erstKvartalsbeskaeftigelse,
                               alchemy_tables.erstMaanedsbeskaeftigelse,
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
        # maybe threadpool is faster...

        def worker(work_idx):
            session = create_session()
            table_class = delete_table_models[work_idx]
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

    @staticmethod
    def delete_employment_only(enh):
        """ Delete from employment tables only """
        delete_table_models = [alchemy_tables.Aarsbeskaeftigelse,
                               alchemy_tables.Kvartalsbeskaeftigelse,
                               alchemy_tables.Maanedsbeskaeftigelse,
                               alchemy_tables.erstAarsbeskaeftigelse,
                               alchemy_tables.erstKvartalsbeskaeftigelse,
                               alchemy_tables.erstMaanedsbeskaeftigelse
                              ]

        def worker(work_idx):
            session = create_session()
            table_class = delete_table_models[work_idx]
            session.query(table_class).filter(table_class.enhedsnummer.in_(enh)).delete(synchronize_session=False)
            session.commit()
            session.close()
        
        threads = []
        for i in range(len(delete_table_models)):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()
    
        for t in threads:
            t.join()
          
    def insert(self, dicts, enh_type):
        """ Insert data from dicts

        Args:
        :param dicts: list of dicts with cvr data (Danish Business Authority)
        :param enh_type: cvr object type
        """
        data_parser = data_scanner.DataParser(_type=enh_type)
        address_parser = self.address_parser_factory.create_parser(self.update_address)
        #print('parse data')
        data_parser.parse_data(dicts)
        #print('parse dynamic data')
        data_parser.parse_dynamic_data(dicts)
        #print('parse address')
        address_parser.parse_address_data(dicts)
        # print('address data inserted/skipped - start static')
        #print('parse static data')
        data_parser.parse_static_data(dicts)
        # print('static parsed')

    def insert_employment_only(self, dicts, enh_type):
        """ Inserts only employment data - needed to to missing version id when employment data updated in CVR"""
        data_parser = data_scanner.DataParser(_type=enh_type)
        data_parser.parse_employment(dicts)
    
    @staticmethod
    def get_samtid_dict(table):
        session = create_session()
        query = session.query(table.enhedsnummer,
                              table.samtid,
                              table.sidstopdateret)
        existing_data = [(x[0], x[1], x[2]) for x in query.all()]
        tmp = {a: CvrConnection.update_info(samtid=b, sidstopdateret=c) for (a, b, c) in existing_data}
        session.close()
        return tmp

    @staticmethod
    def make_samtid_dict():
        """ Make mapping from entity id to current version
        Add threading to run in parallel to see if that increase speed. Use threadpool instad of concurrent_future
        """
        logger = logging.getLogger('cvrparser')
        logger.info('Make id -> samtId map: units update status map')
        table_models = [alchemy_tables.Virksomhed, alchemy_tables.Produktion, alchemy_tables.Person]
        enh_samtid_map = {}

        def worker(table_idx):
            table = table_models[table_idx]
            tmp = CvrConnection.get_samtid_dict(table)
            enh_samtid_map.update(tmp)

        threads = []
        for i in range(len(table_models)):
            t = threading.Thread(target=worker, args=(i, ))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

        logger.info('Id map done')
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
            enh_samtid_map = self.make_samtid_dict()
        dummy = CvrConnection.update_info(samtid=-1, sidstopdateret=self.dummy_date)
        dicts = {x: list() for x in self.source_keymap.values()}
        with open(filename) as f:
            for line in tqdm.tqdm(f):
                raw_dat = json.loads(line)
                keys = raw_dat.keys()
                dict_type_set = keys & self.source_keymap.values()  # intersects the two key sets
                if len(dict_type_set) != 1:
                    add_error('BAD DICT DOWNLOADED {0}'.format(str(raw_dat)))
                    continue
                dict_type = dict_type_set.pop()
                dat = raw_dat[dict_type]
                enhedsnummer = dat['enhedsNummer']
                samtid = dat['samtId']
                if dat['samtId'] is None:
                    add_error('Samtid none. '.format(enhedsnummer))
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

    def get_update_list_single_process(self):
        """ Find units that needs updating and their sidstopdateret (last updated)
        the sidstopdateret may be inaccurate and thus way to far back in time therefore we cannot use take the largest
        of sidstopdateret from the database. Seems we download like 600 dicts a second with match_all.
        Should take around 2 hours and 30 minuttes then. This takes 30 so i need to save half an hour on downloads.

        :return datetime (min sidstopdateret), list (enhedsnumer, sidstopdateret)
        """
        enh_samtid_map = self.make_samtid_dict()
        oldest_sidstopdateret = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)+datetime.timedelta(days=1)
        update_dicts = {x: {'units': [], 'sidstopdateret': oldest_sidstopdateret} for x in self.source_keymap.values()}
        if len(enh_samtid_map) == 0:
            return update_dicts
        dummy = CvrConnection.update_info(samtid=-1, sidstopdateret=self.dummy_date)
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
            params = {'scroll': self.elastic_search_scroll_time, 'size': 2**12}
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

    def get_update_list_type(self, _type):
        return update_time_worker((_type, self.url, self.user, self.password, self.index))

    def get_update_list(self):
        """ Threaded version - may not be so IO wait bound since we stream
        so maybe change to process pool instead """
        pool = Pool(processes=3)
        result = pool.map(update_time_worker, [(x, self.url, self.user, self.password, self.index)
                                               for x in self.source_keymap.values()], chunksize=1)
        update_dicts = {x: y for (x, y) in result}
        print([(k, v['sidstopdateret'], len(v['units'])) for k, v in update_dicts.items()])
        return update_dicts

    def optimize_download_updated(self, update_info):
        """ DEPRECATED

        Due to a missing sidstopdateret for employment updates in cvr
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


def update_time_worker(args):
    _type = args[0]
    url = args[1]
    user = args[2]
    password = args[3]
    index = args[4]
    enh_samtid_map = CvrConnection.make_samtid_dict()
    dummy_date = datetime.datetime(year=1001, month=1, day=1, tzinfo=pytz.utc)
    dummy = CvrConnection.update_info(samtid=-1, sidstopdateret=dummy_date)
    oldest_sidstopdateret = datetime.datetime.utcnow().replace(tzinfo=pytz.utc) + datetime.timedelta(days=1)
    type_dict = {'units': [], 'sidstopdateret': oldest_sidstopdateret}
    if len(enh_samtid_map) == 0:
        return type_dict
    elastic_client = create_elastic_connection(url, (user, password))
    search = Search(using=elastic_client, index=index).query('match_all')
    sidst_key = '{0}.sidstOpdateret'.format(_type)
    samt_key = '{0}.samtId'.format(_type)
    field_list = ['_id', sidst_key, samt_key]
    search = search.fields(fields=field_list)
    params = {'scroll': '10m', 'size': 2 ** 12}
    search = search.params(**params)
    print('ElasticSearch Query: ', search.to_dict())
    generator = search.scan()
    for cvr_update in generator:
        enhedsnummer = int(cvr_update.meta.id)
        raw_dat = cvr_update.to_dict()
        samtid = raw_dat[samt_key][0] if samt_key in raw_dat else None
        sidstopdateret = raw_dat[sidst_key][0] if sidst_key in raw_dat else None
        if sidstopdateret is None or samtid is None:
            continue
        current_update = enh_samtid_map[enhedsnummer] if enhedsnummer in enh_samtid_map else dummy
        if samtid > current_update.samtid:
            utc_sidstopdateret = utc_transform(sidstopdateret)
            type_dict['sidstopdateret'] = min(utc_sidstopdateret, type_dict['sidstopdateret'])
            type_dict['units'].append((enhedsnummer, utc_sidstopdateret))
    return _type, type_dict


def retry_generator(g):
    failed = 0
    while True:
        try:
            yield next(g)
        except StopIteration:
            return
        except Exception as e:
            print('retry generator', failed)
            failed += 1
            print(e)
            if failed > 3:
                raise


def cvr_update_producer(queue, lock):
    """ Producer function that places data to be inserted on the Queue

    :param queue: multiprocessing.Queue
    :param lock: multiprocessing.Lock
    """
    t0 = time.time()

    logger = logging.getLogger('producer')
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('producer.log')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    # add the handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)
    with lock:
        logger.info('Starting producer => {}'.format(os.getpid()))

    if not engine.is_none():
        engine.dispose()        
    else:
        setup_database_connection()
        with lock:
            logger.info('setup databaseconnection - lost in spawn/fork')    

    try:
        cvr = CvrConnection()
        enh_samtid_map = CvrConnection.make_samtid_dict()
        dummy = CvrConnection.update_info(samtid=-1, sidstopdateret=CvrConnection.dummy_date)
        params = {'scroll': cvr.elastic_search_scroll_time, 'size': cvr.elastic_search_scan_size}
        search = Search(using=cvr.elastic_client, index=cvr.index).query('match_all').params(**params)
        # search = Search(using=cvr.elastic_client, index=cvr.index).query(elasticsearch_dsl.query.MatchAll()).params(**params)

        generator = search.scan()
        full_update = False
        i = 0
        for obj in tqdm.tqdm(generator):
            try:
                i = i+1
                dat = obj.to_dict()
                keys = dat.keys()
                dict_type_set = keys & CvrConnection.source_keymap.values()  # intersects the two key sets
                if len(dict_type_set) != 1:
                    print(dict_type_set)
                    logger.debug('BAD DICT DOWNLOADED CVR UPDATE PRODUCER \n{0} {1}'.format(dat, dict_type_set))                    
                    continue
                dict_type = dict_type_set.pop()
                dat = dat[dict_type]
                enhedsnummer = dat['enhedsNummer']
                samtid = dat['samtId']
                if dat['samtId'] is None:
                    add_error('Samtid none: enh {0}'.format(enhedsnummer))
                    dat['samtId'] = -1
                    samtid = -1
                current_update = enh_samtid_map[enhedsnummer] if enhedsnummer in enh_samtid_map else dummy
                
                if samtid > current_update.samtid:
                    full_update = True
                else:
                    if dict_type == 'Vrdeltagerperson':
                        continue
                    full_update = False

                for repeat in range(20):
                    try:
                        queue.put((dict_type, dat, full_update), timeout=60)
                        break
                    except Exception as e:
                        logger.debug('Producer timeout failed {0} - retrying {1} - {2} - repeat: {3} - queue full {4} (unreliable)'.format(str(e), enhedsnummer, dict_type, repeat, queue.full()))
                        if repeat > 10:
                            raise(e)
                if (i % 30000 == 0):
                    logger.debug('{0} rounds'.format(i))                    

            except Exception as e:
                logger.debug('Producer exception: e: {0} - obj: {1}'.format(e, obj))
                print('continue producer')
                # print(obj)
            # if ((i+1) % 10000) == 0:
            #     with lock:
            #         print('{0} objects parsed and inserted into queue'.format(i))
    except Exception as e:
        print('*** generator error ***', file=sys.stderr)
        logger.debug('generator error: {0}'.format(str(e)))
        print(e)
        print(type(e))
        #logger.info(e)
        #logger.info(type(e))
        return
    # Synchronize access to the console
    with lock:
        logger.info('objects parsing done')

    t1 = time.time()
    with lock:
        logger.info('Producer Done. Exiting...{0}'.format(os.getpid()))
        logger.info('Producer Time Used: {0}'.format(t1-t0))
    # queue.put(cvr.cvr_sentinel)
    #    queue.put(cvr.cvr_sentinel)


def test_producer():
    print('test producer')

    class dumqueue():
        def __init__(self):
            self.counter = {}

        def put(self, obj, timeout=None):
            dict_type = obj[0]
            if dict_type in self.counter:
                self.counter[dict_type] += 1
            else:
                self.counter[dict_type] = 1
            #dat = obj[1]


    class dumlock():
        def __enter__(self):
            pass

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    cvr_update_producer(dumqueue(), dumlock())


def cvr_update_consumer(queue, lock):
    """

    :param queue: multiprocessing.Queue
    :param lock: multiprocessing.Lock
    :return:
    """

    t0 = time.time()


    logger = logging.getLogger('consumer-{0}'.format(os.getpid()))
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('consumer_{0}.log'.format(os.getpid()))
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    # add the handlers to logger
    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.info('Starting consumer => {}'.format(os.getpid()))

    if not engine.is_none():        
        engine.dispose()        
    else:
        setup_database_connection()
        with lock:
            logger.info('setup database connection - lost in spawn/fork')    
    
    cvr = CvrConnection()
    # enh_samtid_map = CvrConnection.make_samtid_dict()
    # dummy = CvrConnection.update_info(samtid=-1, sidstopdateret=CvrConnection.dummy_date)
    dicts = {x: list() for x in CvrConnection.source_keymap.values()}
    emp_dicts = {x: list() for x in CvrConnection.source_keymap.values()}
    i = 0
    while True:
        # try:
        i = i+1        
        for repeats in range(30):
            obj = None
            try:
                #logger.info('get data {0}'.format(i))
                obj = queue.get(timeout=10)
                break
            except Exception as e:
                logger.debug('Consumer timeout reached - repeats {1}, retrying - e: {0} '.format(e, repeats))
        try:  # move this
            if obj == cvr.cvr_sentinel:
                logger.info('sentinel found - Thats it im out of here')
                # queue.put(obj)
                break
            elif obj == cvr.cvr_nothing:
                logger.debug('Nothing returned for consumer in long time - breaking')
                break
            if obj is None:
                continue
            #logger.info('Got some data {0}'.format(i))
            assert len(obj) == 3, 'obj not length 2 - should be tuple of length 3'
            dict_type = obj[0]
            dat = obj[1]
            full_update = obj[2]
            if full_update:
                dicts_to_use = dicts
            else:
                dicts_to_use =  emp_dicts
            dicts_to_use[dict_type].append(dat)
            if len(dicts_to_use[dict_type]) >= cvr.update_batch_size:
                #logger.info('try to insert {0}'.format(i))
                t0 = time.time()
                if full_update:
                    cvr.update(dicts_to_use[dict_type], dict_type)
                else:
                    cvr.update_employment_only(dicts_to_use[dict_type], dict_type)
                used_time = time.time() - t0
                #logger.info(' - {0} time used - data inserted {1}'.format(used_time, len(dicts_to_use[dict_type])))
                dicts_to_use[dict_type].clear()
        except Exception as e:
            logger.debug('Exception in consumer: {0} - {1}'.format(os.getpid(), str(e)))
            logger.debug('insert one by one')
            print('Exception in consumer: {0} - {1}'.format(os.getpid(), str(e)))
            for enh_type, _dicts in dicts.items():
                for one_dict in _dicts:
                    logger.debug('inserting {0}'.format(one_dict['enhedsNummer']))
                    try:
                        if full_update:
                            cvr.update([one_dict], enh_type)
                        else:
                            logger.debug('error in emp only')
                            cvr.update_employment_only([one_dict], enh_type)
                    except Exception as e:
                        logger.debug('one insert error\n{0}'.format(str(e)))
                        logger.debug('enh failed: {0}'.format(one_dict['enhedsNummer']))
        if i % 10000 == 0:
            logger.debug('Consumer {0} rounds completed and alive - '.format(i))

        # except Exception as e:
        #     print('Consumer exception', e)
        #     import pdb
        #     pdb.set_trace()
    logger.debug('Consumer empty cache')
    for enh_type, _dicts in dicts.items():
        if len(_dicts) > 0:
            cvr.update(_dicts, enh_type)
    for enh_type, _dicts in emp_dicts.items():
        if len(_dicts) > 0:
            cvr.update_employment_only(_dicts, enh_type)
    t1 = time.time()
    with lock:
        print('Consumer Done. Exiting...{0} - time used {1}'.format(os.getpid(), t1-t0))
