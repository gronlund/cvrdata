import pytz
from bs4 import BeautifulSoup
import dateutil
import datetime
from dateutil.parser import parse as date_parse
from .sql_help import SessionInsertCache, SessionKeystoreCache
from .adresse import beliggenhedsadresse_to_str
from . import alchemy_tables
from .bug_report import add_error
import threading


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


def parse_sidst_opdateret(st):
    utc_sidstopdateret = utc_transform(st['sidstOpdateret']) if st['sidstOpdateret'] is not None else None
    return utc_sidstopdateret


def fast_time_transform(time):
    """ transform strings like 2017-01-29T13:06:04.000+01:00 fast
                               2014-10-02T20:00:00.000Z
    :param time: str, with utc time
    :return: datetime
    """
    val = time[0:28]

    if len(val) > 23 and val[23] != 'Z':
        utc_sign = val[23]
        minute = -int(val[27:29])
        hour = -int(val[24:26])
        if utc_sign == '+':
            tzinfo = dateutil.tz.tzoffset(None, hour * 60 * 60 + minute * 60)
        else:
            tzinfo = dateutil.tz.tzoffset(None, hour * 60 * 60 + minute * 60)
    else:
        tzinfo = pytz.utc
    return datetime.datetime(
            year=int(val[0:4]),  # %Y
            month=int(val[5:7]),  # %m
            day=int(val[8:10]),  # %d
            hour=int(val[11:13]),  # +hour,  # %H
            minute=int(val[14:16]),  # +minute,  # %M
            second=int(val[17:19]),  # %s
            microsecond=int(val[20:23]),  # microseconds
            tzinfo=tzinfo).astimezone(pytz.utc)
    # tzinfo = dateutil.tz.tzoffset(None, hour * 60 * 60)
    # int(val[24:26],   # utc offset hour
    # int(val[27:29]))  # utc offset minute


def slow_time_transform(s):
    """ slow transform of string to time

    :param s: str,
    :return: datetime of s
    """
    try:
        d = date_parse(s[0:28])
        if d.utcoffset() is not None:
            return d.astimezone(pytz.utc)
        else:
            add_error('naive date given - do not do that!!! - i will assume it is utc time anyways: {0}'.format(s))
            # assert False, d
            return d.replace(tzinfo=pytz.utc)
    except Exception as e:
        add_error('Exception utctransform: {0} {1}'.format(e, s))
        return None


def utc_transform(s):
    """ transform string to utc datetime

    :param s: string representation of datetime with utc info
    :return: datetime in utc timezone
    """
    try:
        return fast_time_transform(s)
    except Exception as e:
        add_error('fast transform error: {0} - {1}'.format(s, e))
    return slow_time_transform(s)


class ParserInterface(object):
    """ Trivial interface for parser object """
    def insert(self, data):
        raise NotImplementedError('Implement in subclass')

    def commit(self):
        raise NotImplementedError('Implement in subclass')


class Parser(ParserInterface):
    """ Abstract class for parsing cvr data with sql_cache.
    Change db type with keystore existing or not
    """
    def __init__(self, table_class, columns, keystore=None):
        if keystore is None:
            self.db = SessionInsertCache(table_class, columns)
        else:
            self.db = SessionKeystoreCache(table_class, columns, keystore=keystore)

    def insert(self, data):
        raise NotImplementedError('Implement in subclass')

    def commit(self):
        self.db.commit()


class ParserList(ParserInterface):
    """ Simple class for storing list of parser objects
        using threading for commits. Consider bounding thread count. May not matter.
    """

    def __init__(self):
        self.listeners = []

    def insert(self, data):
        for l in self.listeners:
            l.insert(data)

    def commit(self):
        
        def worker(list_index):
            self.listeners[list_index].commit()

        threads = []
        for i in range(len(self.listeners)):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    def add_listener(self, obj):
        self.listeners.append(obj)


class StaticParser(Parser):
    """ Simple class for parsing static erst data """

    def __init__(self, table_class, json_fields, json_timestamps, table_columns):
        super().__init__(table_class, table_columns)
        self.timestamps = json_timestamps
        self.json_fields = json_fields

    def insert(self, data):
        dat = [data[x] if x in data else None for x in self.json_fields]
        time_dat = [data[x] if (x in data and data[x] is not None) else None for x in self.timestamps]
        time_dat = [utc_transform(x) if x is not None else None for x in time_dat]
        dat = tuple(dat + time_dat)
        self.db.insert(dat)


class UploadData(Parser):
    """ Simple class for uploading value data values to database """

    def __init__(self, table_class, columns, json_fields, key, data_fields, mapping, key_type=lambda x: x):
        super().__init__(table_class=table_class, columns=columns, keystore=mapping)
        self.json_fields = json_fields
        self.key = key
        self.data_fields = data_fields
        self.keystore = mapping
        self.key_type = key_type
        if mapping is None:
            assert False

    def insert(self, data):
        for f in self.json_fields:
            if f not in data:
                continue
            for z in data[f]:
                ukey = self.key_type(z[self.key])
                if type(ukey) is str:
                    ukey = ukey.strip()
                if ukey is None or ukey in self.keystore:
                    continue
                self.keystore.add(ukey)
                hb = tuple(z[df].strip() if type(z[df]) is str else z[df] for df in self.data_fields)
                self.db.insert((ukey, hb))


class UploadBrancheData(Parser):

    def __init__(self, keystore):
        columns = [alchemy_tables.Branche.branchekode, alchemy_tables.Branche.branchetekst]
        super().__init__(table_class=alchemy_tables.Branche, columns=columns, keystore=keystore)
        self.json_fields = ['hovedbranche', 'bibranche1', 'bibranche2', 'bibranche3']
        self.keystore = keystore

    def insert(self, data):
        for f in self.json_fields:
            if f not in data:
                continue
            for z in data[f]:
                try:
                    key = (int(z['branchekode']), z['branchetekst'].strip())
                    if key in self.keystore:
                        continue
                    self.keystore.add(key)
                    self.db.insert((key, key))
                except Exception as e:
                    add_error('bad key in upload branche {0} {1}'.format(e, z))


class IdentityDict(object):
    def __getitem__(self, item):
        return item


class UpdateMapping(object):

    def __init__(self, json_field, key, field_type, field_map=None, key_map=str.strip):
        """
        :param json_field: str, field to extract data from (from dict root)
        :param key: str, id of data field to extract
        :param field_type: str, database enum type of data to insert (see create_cvr_tables)
        :param field_map: dict, map data to something else
        """
        self.json_field = json_field
        self.key = key
        self.field_type = field_type
        if field_map is None:
            self.field_map = IdentityDict()
        else:
            self.field_map = field_map

    def __str__(self):
        if type(self.key) is str:
            return ' '.join([self.json_field, self.field_type, self.key])
        return 'tuple key: ' + ' '.join([self.json_field, self.field_type]+list(self.key))


class UploadMappedUpdates(Parser):

    def __init__(self):
        """ Class for uploading data to cvr updates table
        """
        self.updatemap_list = []
        table = alchemy_tables.Update
        columns = [table.enhedsnummer,
                   table.felttype,
                   table.kode,
                   table.gyldigfra,
                   table.gyldigtil,
                   table.sidstopdateret]
        super().__init__(table, columns, keystore=None)

    def add_mapping(self, mapping):
        """

        :param mapping: UpdateMapping
        :return:
        """
        self.updatemap_list.append(mapping)

    def insert(self, data):
        enh = data['enhedsNummer']
        upload = []
        for update_mapping in self.updatemap_list:
            for z in data[update_mapping.json_field]:
                if type(update_mapping.key) is tuple:
                    # branche - know the types not pretty
                    val = (int(z[update_mapping.key[0]]), z[update_mapping.key[1]].strip())
                    # if val[0] is None or val[1] is None:
                    #     continue
                else:
                    val = z[update_mapping.key]
                    if val is None:
                        continue
                    if type(val) is str:
                        val = val.strip()
                dat = update_mapping.field_map[val]
                tfrom, tto, utc_sidst_opdateret = get_date(z)
                tup = (enh, update_mapping.field_type, dat, tfrom, tto, utc_sidst_opdateret)
                upload.append(tup)
            # remove duplicates
        for x in set(upload):
            self.db.insert(x)


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
        columns.append(table_class.sidstopdateret)
        super().__init__(table_class, columns, keystore=None)
        self.dict_field = dict_field
        self.keys = keys

    def insert(self, data):
        enh = data['enhedsNummer']
        if self.dict_field not in data:
            # print('Error field missing {0} - {1}'.format(enh, self.dict_field))
            return
        for entry in data[self.dict_field]:
            sidstopdateret = parse_sidst_opdateret(entry)
            dat = tuple([enh] + [entry[x] for x in self.keys] + [sidstopdateret])
            self.db.insert(dat)


def get_upload_employment_year():
    """ Simple parser for yearly employment intervals """
    table = alchemy_tables.Aarsbeskaeftigelse
    aar_columns = [table.enhedsnummer, table.aar,
                   table.aarsvaerk, table.ansatte, table.ansatteinklusivejere,
                   table.aarsvaerkinterval, table.ansatteinterval, table.ansatteinklusivejereinterval]
    aar_field = 'aarsbeskaeftigelse'
    aar_keys = ['aar',
                'antalAarsvaerk', 'antalAnsatte', 'antalInklusivEjere',
                'intervalKodeAntalAarsvaerk', 'intervalKodeAntalAnsatte', 'intervalKodeAntalInklusivEjere']
    afp = UploadEmployment(aar_field, aar_keys, table, aar_columns)
    return afp


def get_upload_employment_quarter():
    """ Simple parser for quarterly employment intervals """
    table = alchemy_tables.Kvartalsbeskaeftigelse
    
    kvar_keys = ['aar', 'kvartal',
                 'antalAarsvaerk', 'antalAnsatte',
                 'intervalKodeAntalAarsvaerk', 'intervalKodeAntalAnsatte']
    kvar_field = 'kvartalsbeskaeftigelse'
    kvar_columns = [table.enhedsnummer, table.aar, table.kvartal,
                    table.aarsvaerk, table.ansatte,
                    table.aarsvaerkinterval, table.ansatteinterval]
    kfp = UploadEmployment(kvar_field, kvar_keys, table, kvar_columns)
    return kfp


def get_upload_employment_month():
    table = alchemy_tables.Maanedsbeskaeftigelse
    mnd_field = 'maanedsbeskaeftigelse'
    mnd_keys = ['aar', 'maaned',
                'antalAarsvaerk', 'antalAnsatte',
                'intervalKodeAntalAarsvaerk', 'intervalKodeAntalAnsatte']
    mnd_columns = [table.enhedsnummer, table.aar, table.maaned,
                   table.aarsvaerk, table.ansatte,
                   table.aarsvaerkinterval, table.ansatteinterval]
    mfp = UploadEmployment(mnd_field, mnd_keys, table, mnd_columns)
    return mfp

def get_erst_upload_employment_year():
    """ Simple parser for yearly employment intervals """
    table = alchemy_tables.erstAarsbeskaeftigelse
    aar_columns = [table.enhedsnummer, table.aar,
                   table.aarsvaerk, table.ansatte, table.ansatteinklusivejere,
                   table.aarsvaerkinterval, table.ansatteinterval, table.ansatteinklusivejereinterval]
    aar_field = 'erstAarsbeskaeftigelse'
    aar_keys = ['aar',
                'antalAarsvaerk', 'antalAnsatte', 'antalInklusivEjere',
                'intervalKodeAntalAarsvaerk', 'intervalKodeAntalAnsatte', 'intervalKodeAntalInklusivEjere']
    afp = UploadEmployment(aar_field, aar_keys, table, aar_columns)
    return afp


def get_erst_upload_employment_quarter():
    """ Simple parser for quarterly employment intervals """
    table = alchemy_tables.erstKvartalsbeskaeftigelse
    
    kvar_keys = ['aar', 'kvartal',
                 'antalAarsvaerk', 'antalAnsatte',
                 'intervalKodeAntalAarsvaerk', 'intervalKodeAntalAnsatte']
    kvar_field = 'erstKvartalsbeskaeftigelse'
    kvar_columns = [table.enhedsnummer, table.aar, table.kvartal,
                    table.aarsvaerk, table.ansatte,
                    table.aarsvaerkinterval, table.ansatteinterval]
    kfp = UploadEmployment(kvar_field, kvar_keys, table, kvar_columns)
    return kfp


def get_erst_upload_employment_month():
    table = alchemy_tables.erstMaanedsbeskaeftigelse
    mnd_field = 'erstMaanedsbeskaeftigelse'
    mnd_keys = ['aar', 'maaned',
                'antalAarsvaerk', 'antalAnsatte',
                'intervalKodeAntalAarsvaerk', 'intervalKodeAntalAnsatte']
    mnd_columns = [table.enhedsnummer, table.aar, table.maaned,
                   table.aarsvaerk, table.ansatte,
                   table.aarsvaerkinterval, table.ansatteinterval]
    mfp = UploadEmployment(mnd_field, mnd_keys, table, mnd_columns)
    return mfp

def get_employment_list():
    vp = []
    vp.append(get_upload_employment_year())
    vp.append(get_upload_employment_quarter())
    vp.append(get_upload_employment_month())
    vp.append(get_erst_upload_employment_year())
    vp.append(get_erst_upload_employment_quarter())
    vp.append(get_erst_upload_employment_month())
    return vp


def get_employment_parser():
    vp = ParserList()
    vp.add_listener(get_upload_employment_year())
    vp.add_listener(get_upload_employment_quarter())
    vp.add_listener(get_upload_employment_month())
    vp.add_listener(get_erst_upload_employment_year())
    vp.add_listener(get_erst_upload_employment_quarter())
    vp.add_listener(get_erst_upload_employment_month())
    return vp



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
                dat = tuple(dat1 + [vaerdi, tfrom, tto, utc_sidstopdateret])
                upload.append(dat)
        [self.db.insert(x) for x in set(upload)]


class RegistrationParser(Parser):
    """ Class for parsing raw registrations from Danish Business Authority 
     Example data
     {        'adresse': 'Kompagnistræde 20C, 1208 København K',
              'cvrNummer': '35865497',
              'hovednavn': 'GOMORE ApS',
              'kommunekode': 101,
              'offentliggoerelseId': 27147181,
              'offentliggoerelseTidsstempel': '2019-05-15T11:43:00.528Z',
              'opdateret': '2019-05-15T11:43:00.530Z',
              'oprettet': '2019-05-15T11:43:00.530Z',
              'postnummer': 1208,
              'registreringTidsstempel': '2019-05-15T11:43:00.000Z',
              'sidstOpdateret': '2019-05-15T11:43:05.537Z',
              'tekst': '<?xml version="1.0" encoding="UTF-8"?>\n'
                       '<html>\n'
                       '<head />\n'
                       '<body>Vedtægter ændret: 26.03.2019\n'
                       '<br />\n'
                       '</body></html>',
              'virksomhedsformkode': '80',
              'virksomhedsregistreringstatusser': ['ANDET']},

    """
    keys = ['adresse', 'cvrNummer', 'hovednavn', 'kommunekode', 'offentliggoerelseId', 'offentliggoerelseTidsstempel',  'opdateret', 'oprettet', 'postnummer', 'registreringTidsstempel', 'sidstOpdateret', 'tekst', 'ren_tekst', 'virksomhedsformkode',  'virksomhedsregistreringstatusser']
    timestamps =  ['offentliggoerelseTidsstempel',  'opdateret', 'oprettet', 'registreringTidsstempel', 'sidstOpdateret']
    
    def __init__(self):
        table = alchemy_tables.Registration
        t = table
        #print(t['cvrNummer'])
        #columns = [t[x.lower()] for x in RegistrationParser.keys]
        columns = [t.adresse, t.cvrnummer, t.hovednavn, t.kommunekode, t.offentliggoerelseid ,t.offentliggoerelsetidsstempel, t.opdateret, t.oprettet, t.postnummer, t.registreringtidsstempel, t.sidstopdateret, t.tekst, t.ren_tekst, t.virksomhedsformkode, t.virksomhedsregistreringstatusser]
        super().__init__(table, columns, keystore=None)

    def insert(self, data):
        if type(data) is list:
            for x in data:
                self.insert(x)
            return                
        assert type(data) is dict
        L = data['virksomhedsregistreringstatusser']
        stat = self.parse_status(L)
        data['virksomhedsregistreringstatusser'] =  stat
        if type(data['tekst']) is str:
            data['ren_tekst'] = self.parse_text(data['tekst'])
        else:
            data['ren_tekst'] = None
            
        for z in RegistrationParser.timestamps:
            data[z] = utc_transform(data[z]) if data[z] is not None else None
        data_tuple = tuple([data[key] for key in RegistrationParser.keys])
        self.db.insert(data_tuple)


    def parse_status(self, L):
        if L is None: return None
        if type(L) is str: return L
        assert type(L) is list
        elements = [x for x in L if type(x) is str]
        #bad_elements = [x for x in L if not type(x) is str]
        #print('bad_elements', bad_elements, '\n', data['cvrNummer'])
        if len(elements) == 0: return None
        return ' '.join(elements)
        
    def parse_text(self, text):
        z = BeautifulSoup(text, "lxml").get_text(separator=' ')
        return '\n'.join([x.strip() for x in z.split('\n')])
    
        
class AddressParser(Parser):
    """ Simple wrapper class for parsing adresses """

    def __init__(self, dawa_transl):
        table = alchemy_tables.Adresseupdate
        fields = [table.enhedsnummer, table.adressetype, table.adressematch, table.dawaid, table.gyldigfra,
                  table.gyldigtil, table.post_string,  table.sidstopdateret]
        super().__init__(table, fields)
        # add beligstring to this maybe to see in db
        self.json_fields = ['beliggenhedsadresse', 'postadresse']
        self.best_dawa_match = dawa_transl

    def insert(self, data):
        enh = data['enhedsNummer']
        for field in self.json_fields:
            for _, z in enumerate(data[field]):
                tfrom, tto, utc_sidstopdateret = get_date(z)
                if 'adresseId' in z and z['adresseId'] is not None:
                    aid = z['adresseId']
                    ad_status = 'adresse'
                elif self.best_dawa_match is not None:
                    [aid, ad_status] = self.best_dawa_match.adresse_id(z)
                else:
                    aid = None
                    ad_status = 'No Id'
                bl = beliggenhedsadresse_to_str(z)
                self.db.insert((enh, field, ad_status, aid, tfrom, tto, bl, utc_sidstopdateret))


class ParserFactory(object):
    @staticmethod
    def get_branche_parser(key_store):
        """ Get Branche Parser

        :param key_store: dabai.cvr_scan.mapping
        """
        # branche_config = {
        #     'table_class': alchemy_tables.Branche,
        #     'json_fields': ['hovedbranche', 'bibranche1', 'bibranche2', 'bibranche3'],
        #     'key': 'branchekode',
        #     'key_type': int,
        #     'data_fields': ['branchekode', 'branchetekst'],
        #     'columns': [alchemy_tables.Branche.branchekode, alchemy_tables.Branche.branchetekst],
        #     'mapping': key_store.get_branche_mapping(),
        # }
        return UploadBrancheData(keystore=key_store.get_branche_mapping())
        # return UploadData(**branche_config)

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
