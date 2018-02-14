from sqlalchemy import tuple_
from . import create_session


class MyCache(object):

    def insert(self, val):
        raise NotImplementedError('Overwrite me please')

    def commit(self):
        raise NotImplementedError('Overwrite me please')


class SessionCache(MyCache):

    def __init__(self, table_class, columns, batch_size=2000):
        self.columns = columns
        self.fields = [x.name for x in columns]
        self.cache = []
        self.batch_size = batch_size
        self.table_class = table_class

    def insert(self, val):
        # assert type(val) is tuple
        self.cache.append(val)
        if len(self.cache) >= self.batch_size:
            self.commit()


class SessionInsertCache(SessionCache):
    """ Make new Cache on with keystore one without"""

    def __init__(self, table_class, columns, keystore=None, batch_size=2000):
        super().__init__(table_class, columns, batch_size)
        self.keystore = keystore

    def to_dicts(self):
        """ Make data into dicts for bulk insert,
        only insert elements that are missing from database
        """
        if self.keystore is not None:
            missing = self.keystore.update()
            z = [{x: y for (x, y) in zip(self.fields, c)} for (key, c) in self.cache if key in missing]
        else:
            z = [{x: y for (x, y) in zip(self.fields, c)} for c in self.cache]
            # z = [{x: y for (x, y) in zip(self.fields, c)} for (key, c) in self.cache]
        return z

    def commit(self):
        z = self.to_dicts()
        # objs = [self.table_class(**d) for d in z]
        # self.session.add_all(objs)
        session = create_session()
        session.bulk_insert_mappings(self.table_class, z, render_nulls=True)
        session.commit()
        session.close()
        self.cache = []


class SessionUpdateCache(SessionCache):
    """ Simple class for insert on duplicate replace on a specific key set
        It is implemented as simply delete all, insert again
        Inserts must be of the form (key, data)
        where data should not include the key
    """

    def __init__(self, table_class, key_columns, data_columns, batch_size=2000):
        super().__init__(table_class, key_columns+data_columns, batch_size)
        self.key_columns = key_columns
        self.data_columns = data_columns

    def commit(self):
        """ It not exists insert, else update"""
        if len(self.cache) == 0:
            return
        keys = [x[0] for x in self.cache]
        # delete all keys
        flatten_dat = [x+y for (x, y) in self.cache]
        # print(self.session.query(self.table_class).filter(tuple_(*self.key_columns).in_(keys)).statement)
        session = create_session()
        session.query(self.table_class).filter(tuple_(*self.key_columns).in_(keys)).delete(synchronize_session=False)
        session.expire_all()
        z = [{x: y for (x, y) in zip(self.fields, c)} for c in flatten_dat]
        # insert them again
        session.bulk_insert_mappings(self.table_class, z, render_nulls=True)
        session.commit()
        session.close()
        self.cache = []
