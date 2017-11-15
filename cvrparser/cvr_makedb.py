""" Simple script for setting up the  database with CVR data """
import csv
import os
import requests
from . import alchemy_tables
from . import create_views
from .sql_help import SessionCache
from . import config, create_session


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
    def download_dawa():
        """ Download newst dawa file """
        print('Download newest dawa data')
        filename = os.path.join(config['data_path'], 'dawa.csv')
        url = 'https://dawa.aws.dk/adresser?format=csv'
        r = requests.get(url, stream=True)
        with open(filename, 'wb') as f:
            for chunk in r.iter_content():
                f.write(chunk)
        return filename
        # os.system('wget  https://dawa.aws.dk/adresser?format=csv -O {0}'.format(target))

    @staticmethod
    def insert_dawa():
        """ Insert csv file into dawa mysql table 
        Newest data file can be downloaded by getting  https://dawa.aws.dk/adresser?format=csv
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
        with open(filename, newline='', encoding='UTF-8') as csvfile:
            ad_reader = csv.reader(csvfile, delimiter=',')
            # tmp = next(ad_reader)
            db = SessionCache(table, table.__table__.c)
            for i, row in enumerate(ad_reader):
                row2 = [x if x != '' else None for x in row]
                db.insert(tuple(row2))
                if (i % 10000) == 0:
                    print('Adresses inserted: {0}'.format(i))
            db.commit()
