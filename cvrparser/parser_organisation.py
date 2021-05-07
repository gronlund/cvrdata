import pdb
from .field_parser import Parser, ParserInterface, get_date
from .sql_help import SessionUpdateCache, SessionInsertCache
from .bug_report import add_error
from . import alchemy_tables


def parse_udggaende(spalt, vaerdi):
    keys = ['udgaaende', 'indgaaende']
    if (len(spalt['udgaaende']) > 0) and (len(spalt['indgaaende']) > 0) and (spalt['udgaaende'] != spalt['indgaaende']):
        print('in != ud', 'lets do something anyways')
        
    for key in keys:
        if len(spalt[key]) == 0:
            continue
        elif len(spalt[key]) > 1:
            print('udgaaende len > 1')
            pdb.set_trace()
        else:
            tmp = spalt[key][0]
            if len(tmp['vaerdier']) != 1:
                print('more than one vaerdier')
                pdb.set_trace()
            if tmp['type'] != 'FUNKTION':
                print('type not funktion')
                pdb.set_trace()
            for v in tmp['vaerdier']:
                if v['vaerdi'] != vaerdi:
                    print('not Spaltning/Fusion in indgaaende/udgaaende')
                    pdb.set_trace()
                if (v['periode'] != spalt['organisationsNavn'][0]['periode']) and \
                        (spalt['organisationsNavn'][0]['periode']['gyldigTil'] is not None):
                    print('periodes not matching', v['periode'], spalt['organisationsNavn'][0]['periode'])
                    pdb.set_trace()


def spalt_parser(dat):
    if 'spaltninger' not in dat:
        print('Spaltning field missing')
    if len(dat['spaltninger']) > 0:
        spalt = dat['spaltninger']
        for s in spalt:
            if len(s['organisationsNavn']) != 1:
                add_error('More Org Names: {0}',format(dat))
            parse_udggaende(s, 'Spaltning')


def fusion_parser(dat):
    if len(dat['fusioner']) > 0:
        fus = dat['fusioner']
        for f in fus:
            if len(f['organisationsNavn']) != 1:
                print('more org navn')
                pdb.set_trace()
            parse_udggaende(f, 'Fusion')


class SpaltFusionIndUdParser(Parser):
    """ Simple """
    def __init__(self):
        table = alchemy_tables.SpaltningFusion
        columns = [table.enhedsnummer, table.enhedsnummer_organisation, table.spalt_fusion,
                   table.indud, table.gyldigfra, table.gyldigtil, table.sidstopdateret]
        super().__init__(table,  columns)

    def insert(self, organisationer=None, enhedsnummer=-1):
        updates = []
        for org in organisationer:
            for ind in org['indgaaende']:
                for val in ind['vaerdier']:
                    tfrom, tto, utc_sidstopdateret = get_date(val)
                    dat = (enhedsnummer, org['enhedsNummerOrganisation'], val['vaerdi'].lower(),
                           'indgaaende', tfrom, tto, utc_sidstopdateret)
                    updates.append(dat)
            for ud in org['udgaaende']:
                for val in ud['vaerdier']:
                    tfrom, tto, utc_sidstopdateret = get_date(val)
                    dat = (enhedsnummer, org['enhedsNummerOrganisation'], val['vaerdi'].lower(),
                           'udgaaende', tfrom, tto, utc_sidstopdateret)
                    updates.append(dat)
        updates = list(set(updates))
        for update in updates:
            self.db.insert(update)


class SpaltningFusionParser(ParserInterface):
    """ Class for handling Spaltnig and Fusion"""
    def __init__(self):
        self.indud_parser = SpaltFusionIndUdParser()

    def insert(self, dat):
        """ insert spaltning/fusion data

        :param dat: dict of cvr data
        """
        spalt_parser(dat)
        fusion_parser(dat)
        enhedsnummer_virksomhed = dat['enhedsNummer']
        spaltninger = dat['spaltninger']
        if len(spaltninger) > 0:
            self.indud_parser.insert(spaltninger, enhedsnummer_virksomhed)
        fusioner = dat['fusioner']
        if len(fusioner) > 0:
            self.indud_parser.insert(fusioner, enhedsnummer_virksomhed)

    def commit(self):
        self.indud_parser.commit()


class CompanyOrganisationParser(ParserInterface):
    """ 
    {'deltager': { enhedsNummer, enhedstype,...}
     'kontorsteder: [{...}]
     'organisationer': [{...}]
    """
    def __init__(self):
        self.name_parser = OrganisationNavnParser()

    def insert(self, dat):
        """ insert organisation cvr data into database

        :param dat: dict with cvr data
        """
        if 'deltagerRelation' in dat:
            relations = dat['deltagerRelation']
            for relation in relations:
                organisationer = relation['organisationer']
                self.name_parser.insert(organisationer)
        if ('spaltninger' in dat) and (len(dat['spaltninger']) > 0):
            self.name_parser.insert(dat['spaltninger'], 'spaltning')
        if 'fusioner' in dat and (len(['fusioner']) > 0):
            self.name_parser.insert(dat['fusioner'], 'fusion')

    def commit(self):
        self.name_parser.commit()


class PersonOrganisationParser(ParserInterface):
    """ Parse Organisation Objects which are on the form  
    {'organisationer': [...] 
    'virksomhed': {}
    }
    """
    def __init__(self):
        self.org_parser = OrganisationParser()
        self.member_parser = PersonOrganisationMemberParser()

    def insert(self, dat):
        relations = dat['virksomhedSummariskRelation']
        self.org_parser.insert(relations)
        self.member_parser.insert(dat)

    def commit(self):
        self.org_parser.commit()
        self.member_parser.commit()




class OrganisationNavnParser(ParserInterface):
    """
    [{'navn': 'Direktion',
     'periode': {'gyldigFra': '2006-12-29',
     'gyldigTil': None},
     'sidstOpdateret': '2015-02-10T00:00:00.000+01:00'}]}]    
    """
    def __init__(self):
        """ Table should be OrganisationNavn"""
        table = alchemy_tables.Organisation
        data_columns = [table.gyldigfra, table.gyldigtil, table.sidstopdateret]
        key_columns = [table.enhedsnummer, table.hovedtype, table.navn]
        self.db = SessionUpdateCache(table_class=table, data_columns=data_columns, key_columns=key_columns)
        self.key_store = set()

    def insert(self, organisationer, value='MISSING_HOVEDTYPE'):
        for org in organisationer:
            # if 'enhedsNummerOrganisation' not in org:
            #     import pdb
            #     pdb.set_trace()
            enhedsnummer_org = org['enhedsNummerOrganisation']
            hovedtype = org['hovedtype'] if 'hovedtype' in org else value
            for navn in org['organisationsNavn']:
                key = (enhedsnummer_org, hovedtype, navn['navn'])
                if key in self.key_store:
                    continue
                self.key_store.add(key)
                tfrom, tto, utc_sidstopdateret = get_date(navn)
                self.db.insert((key, (tfrom, tto, utc_sidstopdateret)))

    def commit(self):
        self.db.commit()


class OrganisationAttributParser(Parser):
    """ 
    organisationer: [{
        attributter: [{sekvensnr': 0, 'type': 'FUNKTION', 
                     'vaerdier': [{'periode': {'gyldigFra': '2006-12-29', 'gyldigTil': None},
                                  'sidstOpdateret': '2015-02-10T00:00:00.000+01:00', 'vaerdi': 'Direktion'}], 
        'vaerditype': 'string'}]
     """
    def __init__(self):
        table = alchemy_tables.Attributter
        columns = [table.enhedsnummer, table.sekvensnr, table.vaerdinavn, table.vaerditype,
                   table.vaerdi, table.gyldigfra, table.gyldigtil]
        super().__init__(table, columns)

    def insert(self, organisationer):
        for org in organisationer:
            for att in org['attributter']:
                for vaerdi in att['vaerdier']:
                    tfrom, tto, utc_sidstopdateret = get_date(vaerdi)
                    tup = (org['enhedsNummerOrganisation'], att['sekvensnr'], att['type'], att['vaerditype'],
                           vaerdi['vaerdi'], tfrom, tto)
                    if tup[0] == 0:
                        continue
                        # print('enhedsnummerOrganisation 0 - attributter - skipping')
                    else:
                        self.db.insert(tup)


class OrganisationMemberParser(ParserInterface):
    """
    Parse cvr medlemsData dict
    'medlemsData': [{'attributter': [{'sekvensnr': 0,
                                      'type': 'FUNKTION',
                                      'vaerdier': [{'periode': {'gyldigFra': '2006-12-29', 'gyldigTil': None},
                                                    'sidstOpdateret': '2015-02-10T00:00:00.000+01:00',
                                                    'vaerdi': 'adm. dir'}],        
                                      'vaerditype': 'string'}]}] """
    def __init__(self):
        """ Table should be Enhedsrelation table"""
        table = alchemy_tables.Enhedsrelation
        key_columns = [table.enhedsnummer_virksomhed, table.enhedsnummer_deltager,
                       table.enhedsnummer_organisation, table.sekvensnr, table.vaerdinavn, table.vaerdi,
                       table.gyldigfra]
        # does not work because this is not the key
        data_columns = [table.gyldigtil, table.sidstopdateret]
        # self.db = SessionUpdateCache(table_class=table, key_columns=key_columns, data_columns=data_columns)
        self.db = SessionInsertCache(table_class=table, columns=key_columns+data_columns)
        self.std_types = {'VALGFORM', 'FUNKTION', 'FORRETNINGSADRESSE', 'SUPPLEANT_FOR_DELTAGER_NR',
                          'EJERANDEL_MEDDELELSE_DATO', 'EJERANDEL_PROCENT', 'EJERANDEL_STEMMERET_PROCENT',
                          'STIFTELSESINFORMATION', 'UDNÆVNT_AF', 'EJERANDEL_KAPITALKLASSE',
                          'REVISOR_REGISTRERING_STEMMEANDEL_INDEHAVERTYPE', 'REVISOR_REGISTRERING_STEMMEANDEL_PROCENT',
                          'EJERANDEL_DOKUMENT_ID', 'KOMPLEMENTAR_INDSKUDSKAPITAL', 'KOMPLEMENTAR_INDSKUDSVALUTA',
                          'REVISOR_REGISTRERING_STEMMEANDEL_INDEHAVERUNDTAGELSE',
                          'REVISOR_REGISTRERING_GODKENDT8DIREKTIV'}

    def insert(self, organisationer, enhedsnummer_deltager=None, enhedsnummer_company=None):
        for org in organisationer:
            enhedsnummer_org = org['enhedsNummerOrganisation']
            members = org['medlemsData']
            tuple_head = (enhedsnummer_company, enhedsnummer_deltager, enhedsnummer_org)
            inserts = []
            for member in members:
                attributter = member['attributter']
                for att in attributter:
                    k = att['sekvensnr']
                    membertype = att['type']
                    for vaerdi in att['vaerdier']:
                        tfrom, tto, utc_sidstopdateret = get_date(vaerdi)
                        key_tuple = tuple_head + (k, membertype, vaerdi['vaerdi'], tfrom)
                        data_tuple = (tto, utc_sidstopdateret)
                        inserts.append(key_tuple + data_tuple)
            for ins in set(inserts):
                self.db.insert(ins)
            inserts.clear()

    def commit(self):
        self.db.commit()


class CompanyOrganisationMemberParser(ParserInterface):
    def __init__(self):
        self.member_parser = OrganisationMemberParser()

    def insert(self, dat):
        relations = dat['deltagerRelation']
        enhedsnummer_company = dat['enhedsNummer']

        for relation in relations:
            deltager = relation['deltager']
            if deltager is None:
                add_error('CompanyOrganisationMemberParser - Deltager is None: {0}'.format(enhedsnummer_company))
                continue
            enhedsnummer_deltager = deltager['enhedsNummer']
            # kontorsted = relation['kontorsteder']
            organisationer = relation['organisationer']
            self.member_parser.insert(organisationer, enhedsnummer_company=enhedsnummer_company,
                                      enhedsnummer_deltager=enhedsnummer_deltager)

    def commit(self):
        self.member_parser.commit()


class PersonOrganisationMemberParser(ParserInterface):
    def __init__(self):
        self.member_parser = OrganisationMemberParser()

    def insert(self, dat):
        relations = dat['virksomhedSummariskRelation']
        enhedsnummer_deltager = dat['enhedsNummer']
        for relation in relations:
            company = relation['virksomhed']
            enhedsnummer_company = company['enhedsNummer']
            organisationer = relation['organisationer']
            self.member_parser.insert(organisationer, enhedsnummer_company=enhedsnummer_company,
                                      enhedsnummer_deltager=enhedsnummer_deltager)

    def commit(self):
        self.member_parser.commit()


class OrganisationParser(ParserInterface):
    """
    Consists of four parsers
    Parse static info: fields enhedsnummerOrganisation, hovedtype
    Parse name (navn which maybe kind of organisation, i.e. board. Currently in its own table, s
    Parse Attributtes: -
    parse memberData (the connection beween a company and another entity company or person)
    members are parsed by special classes
    """
    def __init__(self):
        assert False, 'DEPRECATED'
        # self.static_parser = OrganisationStaticParser()
        self.name_parser = OrganisationNavnParser()
        # self.attribute_parser = OrganisationAttributParser()
        self.relation_keys = {'virksomhed', 'organisationer', 'deltager', 'kontorsteder'}

    def insert(self, relations):
        # relations = person['virksomhedSummariskRelation']
        for relation in relations:
            if not set(relation.keys()).issubset(self.relation_keys):
                add_error('relation keys wrong: \n{0}'.format( relation.keys()))
            organisationer = relation['organisationer']
            # self.static_parser.insert(organisationer)
            self.name_parser.insert(organisationer)
            # self.attribute_parser.insert(organisationer)

    def commit(self):
        # self.static_parser.commit()
        self.name_parser.commit()
        # self.attribute_parser.commit()
        # self.member_parser.commit()
