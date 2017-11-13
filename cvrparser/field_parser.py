import json
import pytz
from .sql_help import SessionInsertCache
from .adresse import beliggenhedsadresse_to_str
from dateutil.parser import parse as date_parse
from . import alchemy_tables
from .bug_report import add_error

def get_date(st):
    """
    Extract gyldigfra, gyldigtil, sidstopdateret from cvr time interval struct
    {...
    'periode': {'gyldigFra': '1920-03-19', 'gyldigTil': '2012-11-26'},
    'sidstOpdateret': '2015-02-26T00:00:00.000+01:00'},}

    :param st: dict with periode and sidstOpdateret keys
    :return: datetime, datetime, datetime - gyldigfra, gyldigtil, utc_sidstopdateret
    """
    default_start = '1900-01-01'
    default_end = '2200-01-01'
    z = (st['periode']['gyldigFra'], st['periode']['gyldigTil'])
    res = (z[0] if not z[0] is None else default_start, z[1] if z[1] is not None else default_end)
    utc_sidstopdateret = utc_transform(st['sidstOpdateret']) if st['sidstOpdateret'] is not None else None
    return res[0], res[1], utc_sidstopdateret


def utc_transform(s):
    """ transform string to utc datetime

    :param s: string representation of datetime with utc info
    :return: datetime in utc timezone
    """

    try:
        d = date_parse(s)
        if d.utcoffset() is not None:
            return d.astimezone(pytz.utc)
        else:
            add_error('naive date given - do not do that!!! - i will assume it is utc time anyways', -1)
            # assert False, d
            return d.replace(tzinfo=pytz.utc)
    except Exception as e:
        print('Exception utctransform: ', e, s)
        return None


class ParserInterface(object):
    """ Trivial interface for parser object """
    def insert(self, data):
        raise NotImplementedError('Implement in subclass')

    def commit(self):
        raise NotImplementedError('Implement in subclass')


class Parser(object):
    """ Abstract class for parsing cvr data with sql_cache. """
    def __init__(self, table_class, columns, keystore=None):
        self.db = SessionInsertCache(table_class, columns, keystore=keystore)

    def insert(self, data):
        raise NotImplementedError('Implement in subclass')

    def commit(self):
        self.db.commit()


class ParserList(ParserInterface):
    """ Simple class for storing list of parser objects """

    def __init__(self):
        self.listeners = []

    def insert(self, data):
        for l in self.listeners:
            l.insert(data)

    def commit(self):
        for l in self.listeners:
            l.commit()

    def add_listener(self, obj):
        self.listeners.append(obj)


class StaticParser(Parser):
    """ Simple class for parsing static erst data """

    def __init__(self, table_class, json_fields, json_timestamps, table_columns):
        # self.col_names = [x.lower() for x in json_fields + json_timestamps]
        super().__init__(table_class, table_columns)
        self.timestamps = json_timestamps
        self.json_fields = json_fields

    def insert(self, data):

        dat = [data[x] if x in data else None for x in self.json_fields]
        #  time_dat = [date_parse(data[x]).astimezone(tz=None) if
        #  (x in data and data[x] is not None)else None for x in self.timestamps]
        time_dat = [data[x] if (x in data and data[x] is not None)else None for x in self.timestamps]
        # time_dat = [utc_transform(date_parse(x)).strftime('%Y-%m-%d %H:%M:%S.%f')
        #             if x is not None else None for x in time_dat]
        time_dat = [utc_transform(x) if x is not None else None for x in time_dat]

        dat = tuple(dat + time_dat)
        self.db.insert(dat)


class UploadData(Parser):
    """ Simple class for uploading value data values to database """

    def __init__(self, table_class, columns, json_fields, key, data_fields, mapping, key_type=lambda x: x):
        super().__init__(table_class, columns, keystore=mapping)
        self.json_fields = json_fields
        self.key = key
        self.data_fields = data_fields
        self.keystore = mapping
        self.key_type = key_type

    def insert(self, data):
        for f in self.json_fields:
            if f not in data:
                continue
            for z in data[f]:
                ukey = self.key_type(z[self.key])
                if ukey is None or ukey in self.keystore:
                    continue
                self.keystore.add(ukey)
                hb = tuple(z[df] for df in self.data_fields)
                self.db.insert((ukey, hb))


class UploadTimeMap(Parser):
    """ Class for uploading time period  data from data that must be mapped to a predefined key """

    def __init__(self, json_field, key, field_type, field_map):
        """

        :param json_field: field to extract data from (from dict root)
        :param key: id of data field to extract
        :param field_type:  database enum type of data to insert (see create_cvr_tables)
        :param field_map: map to map data to something else
        """
        table = alchemy_tables.Update
        columns = [table.enhedsnummer, table.felttype, table.kode, table.gyldigfra, table.gyldigtil,
                   table.sidstopdateret]
        super().__init__(table, columns, keystore=None)
        self.json_field = json_field
        self.key = key
        self.field_map = field_map
        self.field_type = field_type

    def insert(self, data):
        enh = data['enhedsNummer']
        upload = []
        for z in data[self.json_field]:
            val = z[self.key]
            if val is None:
                continue
            dat = self.field_map[val]
            tfrom, tto, utc_sidst_opdateret = get_date(z)
            tup = (enh, self.field_type, dat, tfrom, tto, utc_sidst_opdateret)
            upload.append(tup)
        # remove duplicates
        [self.db.insert(x) for x in set(upload)]


class IdentityDict(object):
    def __getitem__(self, item):
        return item


class UploadTimeDirect(UploadTimeMap):
    """ Class for directly uploading data values"""

    def __init__(self, json_field, key, field_type):
        super().__init__(json_field, key, field_type, IdentityDict())


class UploadLivsforloeb(Parser):
    """ Simple class for parsing livsforloeb """

    def __init__(self, ):
        table = alchemy_tables.Livsforloeb
        columns = [table.enhedsnummer, table.gyldigfra, table.gyldigtil, table.sidstopdateret]
        super().__init__(table, columns, keystore=None)

    def insert(self, data):
        enh = data['enhedsNummer']
        for z in data['livsforloeb']:
            tfrom, tto, utc_sidstopdateret = get_date(z)
            dat = tuple([enh, tfrom, tto, utc_sidstopdateret])
            self.db.insert(dat)


class UploadEmployment(Parser):
    """ Simple class for parsing employment from cvr data file """

    def __init__(self, dict_field, keys, table_class, columns):
        super().__init__(table_class, columns, keystore=None)
        self.dict_field = dict_field
        self.keys = keys

    def insert(self, data):
        enh = data['enhedsNummer']
        if self.dict_field not in data:
            # print('Error field missing {0} - {1}'.format(enh, self.dict_field))
            return
        for entry in data[self.dict_field]:
            dat = tuple([enh] + [entry[x] for x in self.keys])
            self.db.insert(dat)


def get_upload_employment_year():
    """ Simple parser for yearly employment intervals """
    aar_class = alchemy_tables.AarsbeskaeftigelseInterval
    aar_table = alchemy_tables.AarsbeskaeftigelseInterval
    aar_columns = [aar_table.enhedsnummer, aar_table.aar, aar_table.aarsvaerk, aar_table.ansatte,
                   aar_table.ansatteinklusivejere]
    aar_field = 'aarsbeskaeftigelse'
    aar_keys = ['aar', 'intervalKodeAntalAarsvaerk', 'intervalKodeAntalAnsatte', 'intervalKodeAntalInklusivEjere']
    afp = UploadEmployment(aar_field, aar_keys, aar_class, aar_columns)
    return afp


def get_upload_employment_quarter():
    """ Simple parser for quarterly employment intervals """
    kvar_class = alchemy_tables.KvartalsbeskaeftigelseInterval
    kvar_table = alchemy_tables.KvartalsbeskaeftigelseInterval
    kvar_keys = ['aar', 'kvartal', 'antalAarsvaerk', 'antalAnsatte']
    kvar_field = 'kvartalsbeskaeftigelse'
    kvar_columns = [kvar_table.enhedsnummer, kvar_table.aar, kvar_table.kvartal, kvar_table.aarsvaerk,
                    kvar_table.ansatte]
    kfp = UploadEmployment(kvar_field, kvar_keys, kvar_class, kvar_columns)
    return kfp


def get_upload_employment_month():
    mnd_class = alchemy_tables.MaanedsbeskaeftigelseInterval
    mnd_table = alchemy_tables.MaanedsbeskaeftigelseInterval
    mnd_field = 'maanedsbeskaeftigelse'
    mnd_keys = ['aar', 'maaned', 'intervalKodeAntalAarsvaerk', 'intervalKodeAntalAnsatte']
    mnd_columns = [mnd_table.enhedsnummer, mnd_table.aar, mnd_table.maaned, mnd_table.aarsvaerk,
                   mnd_table.ansatte]
    mfp = UploadEmployment(mnd_field, mnd_keys, mnd_class, mnd_columns)
    return mfp


class AttributParser(Parser):
    """ class for parsing cvr attributter objects """

    def __init__(self):
        table = alchemy_tables.Attributter
        columns = [table.enhedsnummer, table.sekvensnr, table.vaerdinavn, table.vaerditype,
                   table.vaerdi, table.gyldigfra, table.gyldigtil]
        super().__init__(table, columns, keystore=None)

    def insert(self, data):
        enh = data['enhedsNummer']
        upload = []
        for att in data['attributter']:
            dat1 = [enh, att['sekvensnr'], att['type'], att['vaerditype']]
            vds = att['vaerdier']
            for vd in vds:
                tfrom, tto, utc_sidstopdateret = get_date(vd)
                vaerdi = vd['vaerdi']
                dat = tuple(dat1+[vaerdi, tfrom, tto, utc_sidstopdateret])
                upload.append(dat)
        [self.db.insert(x) for x in set(upload)]


class AddressParser(Parser):
    """ Simple wrapper class for parsing adresses """

    def __init__(self, dawa_transl, google_transl=None):
        table = alchemy_tables.Adresseupdate
        fields = [table.enhedsnummer, table.adressetype, table.adressematch, table.kode, table.gyldigfra,
                  table.gyldigtil, table.post_string, table.adresse_json, table.sidstopdateret]
        super().__init__(table, fields)
        # add beligstring to this maybe to see in db
        self.json_fields = ['beliggenhedsadresse', 'postadresse']
        self.at = dawa_transl
        self.gl = google_transl
        self.use_google = self.gl is not None

    def insert(self, data):
        enh = data['enhedsNummer']
        for field in self.json_fields:
            for bi, z in enumerate(data[field]):
                tfrom, tto, utc_sidstopdateret = get_date(z)
                [aid, ad_status] = self.at.adresse_id(z)
                # print('what is aid', aid)
                if aid == -1:
                    if ad_status == 'nedlagt_adresse':
                        print('nedlagt adresse')
                        # self.bad_cache.append((enh, bi, hsh))
                    elif ad_status[0:6] == 'udland':
                        print('udlandsk adresse - ignore ', ad_status)
                    elif ad_status == self.at.fail[1] and self.use_google:
                        print('dawa failed - try google', enh, aid, ad_status, z)
                        try:
                            aid = self.gl.google_find(z)
                            if aid == -1:
                                # self.bad_cache.append((enh, bi, hsh))
                                print('No Match')
                            else:
                                ad_status = 'adresse_google_nearest'
                        except Exception as e:
                            print('Google Fail', e)
                            self.use_google = False
                            # self.bad_cache.append((enh, bi, hsh))
                bl = beliggenhedsadresse_to_str(z)
                if aid == -1:  # Make it null as we made no match
                    aid = None
                self.db.insert((enh, field, ad_status, aid, tfrom, tto, bl, json.dumps(z), utc_sidstopdateret))


class ParserFactory(object):
    @staticmethod
    def get_branche_parser(key_store):
        """ Get Branche Parser

        :param key_store: dabai.cvr_scan.mapping
        """
        branche_config = {
            'table_class': alchemy_tables.Branche,
            'json_fields': ['hovedbranche', 'bibranche1', 'bibranche2', 'bibranche3'],
            'key': 'branchekode',
            'key_type': int,
            'data_fields': ['branchekode', 'branchetekst'],
            'columns': [alchemy_tables.Branche.branchekode, alchemy_tables.Branche.branchetekst],
            'mapping': key_store.get_branche_mapping(),
        }
        return UploadData(**branche_config)

    @staticmethod
    def get_navne_parser(key_store):
        """ Get navne parser

        :param key_store: dabai.cvr_scan.mapping
        """
        # navn, binavn
        navn_config = {
            'table_class': alchemy_tables.Navne,
            'json_fields': ['navne', 'binavne'],
            'key': 'navn',
            'data_fields': ['navn'],
            'columns': [alchemy_tables.Navne.navn],
            'mapping': key_store.get_name_mapping()
        }
        return UploadData(**navn_config)

    @staticmethod
    def get_kontakt_parser(key_store):
        """ Get navne parser

        :param key_store: dabai.cvr_scan.mapping
        """
        kontaktinfo_config = {
            'table_class': alchemy_tables.Kontaktinfo,
            'json_fields': ['elektroniskPost', 'telefonNummer', 'telefaxNummer', 'obligatoriskEmail', 'hjemmeside'],
            'key': 'kontaktoplysning',
            'data_fields': ['kontaktoplysning'],
            'columns': [alchemy_tables.Kontaktinfo.kontaktoplysning],
            'mapping': key_store.get_kontakt_mapping()
        }
        return UploadData(**kontaktinfo_config)
