from sqlalchemy import tuple_
from . import create_session
from contextlib import closing
from .bug_report import add_error
import os
import logging

class MyCache(object):
    """ Change to use async inserts perhaps - that would be neat
    https://further-reading.net/2017/01/quick-tutorial-python-multiprocessing/
    """
    def insert(self, val):
        raise NotImplementedError('Overwrite me please')

    def commit(self):
        raise NotImplementedError('Overwrite me please')


class SessionCache(MyCache):

    def __init__(self, table_class, columns, batch_size=1000):
        self.columns = columns
        self.fields = [x.name for x in columns]
        self.cache = []
        self.batch_size = batch_size
        self.table_class = table_class

    def insert(self, val):
        # assert type(val) is tuple
        self.cache.append(val)
        # if len(self.cache) >= self.batch_size:
        #     self.commit()


class SessionInsertCache(SessionCache):
    """ Make new Cache on with keystore one without
       Wrap session around keystore.update
    """

    def __init__(self, table_class, columns,  batch_size=1000):
        super().__init__(table_class, columns, batch_size)

    def commit(self):
        z = [{x: y for (x, y) in zip(self.fields, c)} for c in self.cache]
        # objs = [self.table_class(**d) for d in z]
        # self.session.add_all(objs)
        # t0 = time.time()
        with closing(create_session()) as session:
            session.bulk_insert_mappings(self.table_class, z, render_nulls=True)
            session.commit()
        # t1 = time.time()
        # total = t1 - t0
        # print('insert cache', self.table_class)
        self.cache = []


class SessionKeystoreCache(SessionCache):
    def __init__(self, table_class, columns, keystore, batch_size=1000):
        super().__init__(table_class, columns, batch_size)
        self.keystore = keystore

    def commit(self):
        # t0 = time.time()
        # session = create_session()
        # does not update the keystore which may be a problem
        success = False
        for i in range(3):
            missing = sorted(self.keystore.update())
            session = create_session()
            try:
                z = [{x: y for (x, y) in zip(self.fields, c)} for (key, c) in self.cache if key in missing]
                session.bulk_insert_mappings(self.table_class, z, render_nulls=True)
                session.commit()
                success = True
                break
            except Exception as e:
                #logger = logging.getLogger('consumer-{0}'.format(os.getpid()))
                #logger.debug('Session Keystore Cache Error: e: {0}'.format(e))
                add_error('SessionKeyStoreCache: \n{0} - attempt {1}'.format(e, i))
                session.rollback()

                if i == 2:
                    add_error('SessionKeyStoreCache: \n{0} - attempt {1} - data {2} '.format(e, i, str(z)))
                    for x in z:
                        print(z)
            finally:
                session.close()
        # t1 = time.time()
        # total = t1 - t0
        # print('keystore cache', self.table_class)
        if success:
            self.cache = []
        else:
            raise Exception('CANNOT INSERT {0}'.format(z))


class SessionUpdateCache(SessionCache):
    """ Simple class for insert on duplicate replace on a specific key set
        It is implemented as simply delete all, insert again
        Inserts must be of the form (key, data)
        where data should not include the key
    """

    def __init__(self, table_class, key_columns, data_columns, batch_size=1000):
        super().__init__(table_class, key_columns+data_columns, batch_size)
        self.key_columns = key_columns
        self.data_columns = data_columns

    def commit(self):
        """ It not exists insert, else update
        Has deadlock issue since we delete then insert.
        """
        if len(self.cache) == 0:
            return
        #print('update cache', self.table_class, 'pid')
        self.cache = sorted(self.cache)
        #print(self.cache)
        keys = [x for (x, y) in self.cache]
        # delete all keys
        flatten_dat = [x+y for (x, y) in self.cache]
        z = [{x: y for (x, y) in zip(self.fields, c)} for c in flatten_dat]
        session = create_session()
        for i in range(5):
            try:
                session.query(self.table_class).filter(tuple_(*self.key_columns).in_(keys)).with_for_update().delete(synchronize_session=False)
                
                session.bulk_insert_mappings(self.table_class, z, render_nulls=True)
                session.commit()
                #success = True
                break
            except Exception as e:
                #logger = logging.getLogger('consumer-{0}'.format(os.getpid()))
                #logger.debug('Session Updace Cache Error: e: {0}'.format(e)) 
                #print('session update failure', e)
                session.rollback()
                add_error('SessionUpdateCache: \n{0}'.format(e))
                #logging.info('Deadlock issue in delete insert - RETRY\n{0}'.format(e))
            finally:
                session.close()
        self.cache = []
