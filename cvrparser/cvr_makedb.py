""" Simple script for setting up the  database with CVR data """
import csv
import os
import requests
from tqdm import tqdm
from . import alchemy_tables
from . import create_views
from . import create_session, config
from .sql_help import SessionInsertCache


class MakeCvrDatabase(object):
    def __init__(self):
        self.alchemy_model = alchemy_tables.CreateDatabase()

    def setup_database(self):
        self.create_tables()
        self.create_views()

    def create_tables(self):
        print('---- CREATE TABLES ----')
        self.alchemy_model.create_tables()
        self.alchemy_model.create_update_indexes()

    def create_query_indexes(self):
        print('Creating Query Indexes')
        self.alchemy_model.create_query_indexes()

    @staticmethod
    def create_views():
        print('--- Creating Views --- only with mysql i think')
        create_views.create_views()

    @staticmethod
    def drop_views_and_tables():
        print('not doing much')

    @staticmethod
    def download_dawa():
        """ Download newst dawa file """
        print('Download newest dawa data')
        filename = os.path.join(config['data_path'], 'dawa.csv')
        url = 'https://dawa.aws.dk/adresser?format=csv'
        r = requests.get(url, stream=True)
        total_length = int(r.headers.get('content-length', 0))
        chunk_size = 1024
        with open(filename, 'wb') as f:
            for data in tqdm(r.iter_content(chunk_size=chunk_size),
                             total=int(total_length/chunk_size), unit='KB'):
                f.write(data)
        return filename

    @staticmethod
    def insert_dawa():
        """ Insert csv file into dawa mysql table
        Newest data file can be downloaded by getting
        https://dawa.aws.dk/adresser?format=csv
        """
        print('---- Insert DAWA Address Data ----')
        filename = MakeCvrDatabase.download_dawa()
        print('Clear Existing Table')
        table = alchemy_tables.AdresseDawa
        session = create_session()
        session.query(table).delete()
        session.commit()
        session.close()
        print('Dawa Table Emptied')
        cols = table.__table__.c
        extract = set(x.name for x in cols)
        with open(filename, newline='', encoding='UTF-8') as csvfile:
            csv_reader = csv.DictReader(csvfile, delimiter=',')
            db = SessionInsertCache(table, cols)
            for i, row in enumerate(csv_reader):
                dat = tuple(v if v != '' else None
                            for k, v in row.items() if k in extract)
                db.insert(dat)
                if (i % 1000) == 0:
                    print('Commiting 1000, Total Adresses inserted: {0}'.format(i))
                    db.commit()
            db.commit()
