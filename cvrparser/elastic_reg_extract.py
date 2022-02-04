from elasticsearch import Elasticsearch
# import elasticsearch_dsl
from elasticsearch_dsl import Search
from collections import namedtuple
import datetime
import os
import pytz
import tqdm
import logging
#import threading
#from .field_parser import utc_transform
from . import config, create_session, engine, setup_database_connection
from . import alchemy_tables
from .bug_report import add_error
from . import data_scanner
import multiprocessing
import time
import sys


def update_all_mp(workers=1):
    # https://docs.python.org/3/howto/logging-cookbook.html
    lock = multiprocessing.Lock()
    queue_size = 20000
    queue = multiprocessing.Queue(maxsize=queue_size)  # maxsize=1000*1000*20)
    prod = multiprocessing.Process(target=reg_producer, args=(queue, lock))
    # prod.daemon = True
    prod.start()
    consumers = [multiprocessing.Process(target=reg_consumer, args=(queue, lock))
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
        queue.put(RegistrationConnection.reg_sentinel)
    
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


class RegistrationConnection(object):
    """ Class for connecting and retrieving data from danish CVR register """
    dummy_date = datetime.datetime(year=1001, month=1, day=1, tzinfo=pytz.utc)
    source_keymap = {'virksomhed': 'Vrvirksomhed',
                     'deltager': 'Vrdeltagerperson',
                     'produktionsenhed': 'VrproduktionsEnhed'}
    update_info = namedtuple('update_info', ['samtid', 'sidstopdateret'])
    reg_sentinel = 'REG_SENTINEL'
    cvr_nothing = 'NOTHING_RETURNED'

    def __init__(self):
        """ Setup everything needed for elasticsearch connection to Danish Business Authority for data extraction

        """
        #http://distribution.virk.dk/registreringstekster/registreringstekst/_search
        self.url = 'http://distribution.virk.dk:80'
        self.index = 'registreringstekster'
        self.user = config['cvr_user']
        self.password = config['cvr_passwd']
        self.update_batch_size = 1024

        # self.datapath = config['data_path']
        # self.ElasticParams = [self.url, (self.user, self.password), 60, 10, True]
        self.elastic_client = create_elastic_connection(self.url, (self.user, self.password))
        print('Elastic Search Client:', self.elastic_client.info())
        self.elastic_search_scan_size = 512
        self.elastic_search_scroll_time = u'20m'

    def search_field_val(self, field, value, size=100):
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
        search = search.query('match', **{'cvrNummer': cvrnummer}).extra(size=1000)
        response = search.execute()
        hits = response.hits.hits
        print('len hits', len(hits))
        return hits

    def insert_registrations(self, registrations):
        """ Insert registration data from dicts

        Args:
        :param registrations: list of dicts with cvr data (Danish Business Authority)
        :param enh_type: cvr object type
        """
        data_parser = data_scanner.RegistrationParser()
        data_parser.parse_data(registrations)

    @staticmethod
    def insert_all():
        """ Interface call to start the producer consumder process 
        """
        update_all_mp(3)
        return
    
    @staticmethod
    def get_id_dict():
        table = alchemy_tables.Registration
        session = create_session()
        query = session.query(table.offentliggoerelseid,
                              table.sidstopdateret)
        existing_data = {x[0]: x[1] for x in query.all()}
        return existing_data


def add_logging(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler(f'{name}.log')
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
    return logger

    
def reg_producer(queue, lock):
    """ Producer function that places data to be inserted on the Queue

    :param queue: multiprocessing.Queue
    :param lock: multiprocessing.Lock
    """
    t0 = time.time()
    logger = add_logging('Reg-Producer')
    with lock:
        logger.info('Starting reg producer => {}'.format(os.getpid()))
    if not engine.is_none():
        engine.dispose()        
    else:
        setup_database_connection()
        with lock:
            logger.info('setup databaseconnection - lost in spawn/fork')    

    try:
        regconn = RegistrationConnection()
        enh_samtid_map = RegistrationConnection.get_id_dict()

        params = {'scroll': regconn.elastic_search_scroll_time, 'size': regconn.elastic_search_scan_size}
        search = Search(using=regconn.elastic_client, index=regconn.index).query('match_all').params(**params)

        generator = search.scan()
        i = 0
        for obj in tqdm.tqdm(generator):
            try:
                i = i+1
                dat = obj.to_dict()
                if dat['offentliggoerelseId'] in enh_samtid_map:
                    continue
                for repeat in range(20):
                    try:
                        queue.put(dat, timeout=120)
                        break
                    except Exception as e:
                        logger.debug('Reg-Producer timeout failed {0} - retrying {1} - {2} - repeat: {3}'.format(str(e), enhedsnummer, dict_type, repeat))
                        if repeat > 10:
                            raise(e)
                if (i % 10000 == 0):
                    logger.debug('{0} rounds'.format(i))                    
            except Exception as e:
                logger.debug('Reg-Producer exception: e: {0} - obj: {1}'.format(e, obj))                
    except Exception as e:
        print('*** generator error ***', file=sys.stderr)
        logger.debug('generator error: {0}'.format(str(e)))
        print(e)
        print(type(e))
        return
    # Synchronize access to the console
    with lock:
        logger.info('objects parsing done')

    t1 = time.time()
    with lock:
        logger.info('Reg Producer Done. Exiting...{0}'.format(os.getpid()))
        logger.info('Reg Producer Time Used: {0}'.format(t1-t0))


def reg_consumer(queue, lock):
    """
    For now just inserts all. Fix to update at some point.

    :param queue: multiprocessing.Queue
    :param lock: multiprocessing.Lock
    :return:
    """

    t0 = time.time()
    name = 'Reg-Consumer-{0}'.format(os.getpid())
    logger = add_logging(name)
    logger.info('Starting => {}'.format(name))
    if not engine.is_none():        
        engine.dispose()        
    else:
        setup_database_connection()
        with lock:
            logger.info('setup database connection - lost in spawn/fork')    
    
    regconn = RegistrationConnection()
    i = 0
    data = []
    while True:
        i = i+1        
        for repeats in range(100):
            #obj = regconn.cvr_nothing
            obj = None
            try:
                obj = queue.get(timeout=15)
                break
            except Exception as e:
                logger.debug('Consumer timeout - repeats {1}, retrying - e: {0} '.format(e, repeats))
        try:  # move this
            if obj == regconn.reg_sentinel:
                logger.info('Sentinel found - Thats it im out of here')
                break
            elif obj == regconn.cvr_nothing:
                logger.debug('Nothing returned for consumer in long time - breaking')
                break
            if obj is not None:
                data.append(obj)
            if len(data) >= regconn.update_batch_size:
                #t0 = time.time()
                regconn.insert_registrations(data)            
                #used_time = time.time() - t0
                data = []                
        except Exception as e:
            logger.debug('Exception in consumer: {0} - {1}'.format(os.getpid(), str(e)))
            logger.debug(str(data))
            cvrs = [x['cvrNummer'] for x in data]
            logger.debug(str(cvrs))
            #logger.debug('cvr failed: {0}'.format(x['çvrNummer']))
            #logger.debug(str(data[0]))
        if i % 10000 == 0:
            logger.debug('Consumer {0} rounds completed and alive - times used {1}'.format(i, time.time()-t0))

    logger.debug('Consumer empty cache')
    if len(data) > 0:
        regconn.insert_registrations(data)
    t1 = time.time()
    with lock:
        print('{0} Done. Exiting - time used {1}'.format(name, t1-t0))
