from . import field_parser as fp
from . import parser_organisation
from . import alchemy_tables
from . import create_session
from .bug_report import add_error


class UploadStatusTyper(fp.Parser):
    """ Simple class for handling 2D keys for status typer """
    def __init__(self, key_store):
        table_class = alchemy_tables.Statuskode
        self.keys = ['statuskode', 'kreditoplysningkode', 'statustekst', 'kreditoplysningtekst']
        columns = [table_class.statuskode, table_class.kreditoplysningskode, table_class.statustekst, table_class.kreditoplysningtekst]
        super().__init__(table_class=table_class, columns=columns, keystore=key_store)
        self.keystore = key_store

    def insert(self, data):
        for z in data['status']:
            key = (z['statuskode'], z['kreditoplysningkode'])
            if key in self.keystore or key is None:
                continue
            self.keystore.add(key)
            dat = [z.get(x, 'None') for x in self.keys]
            # dat = (z['statuskode'], z['kreditoplysningkode'])
            self.db.insert((key, dat))


class StatusKoderMap(fp.Parser):
    """ Simple class for parsing statuskoder that is pairs of numbers (statuskode, kreditoplysningskode) 
    Now added fields statustekst, kreditoplysningstekst
    """

    def __init__(self):
        table = alchemy_tables.Update
        columns = [table.enhedsnummer,
                   table.felttype,
                   table.kode,
                   table.gyldigfra,
                   table.gyldigtil,
                   table.sidstopdateret]
        super().__init__(table_class=table, columns=columns, keystore=None)
        session = create_session()
        stat_table = alchemy_tables.Statuskode
        query = session.query(stat_table.statusid,
                              stat_table.statuskode,
                              stat_table.kreditoplysningskode)
        dat = query.all()
        session.close()
        self.field_map = {(y, z): x for (x, y, z) in dat}
        self.field_type = 'status'
    
    def insert(self, data):
        enh = data['enhedsNummer'] 
        for z in data['status']:
            val = (z['statuskode'], z['kreditoplysningkode'])
            if val[0] is None or val[1] is None:
                add_error('Statuskode - bad statuskode: {0}'.format(enh))
                continue
            dat = self.field_map[val]
            tfrom, tto, utc_sidstopdateret = fp.get_date(z)
            dat = (enh, self.field_type, dat, tfrom, tto)
            self.db.insert(dat)


class VirksomhedParserFactory(object):

    def __init__(self, key_store):
        """

        :param key_store: data_scanner.KeyStore
        """
        self.key_store = key_store

    @staticmethod
    def get_static_parser():
        """ Return parser for static data fields
            cvrNummer, dataAdgang, fejlBeskrivelse, fejlRegistreret,
            fejlVedIndlaesning, naermesteFremtidigeDato,
            reklamebeskyttet, samtId, sidstIndlaest,
            sidstOpdateret, virkningsAktoer
        """
        timestamps = ['naermesteFremtidigeDato',
                      'sidstIndlaest', 'sidstOpdateret']
        json_fields = ['enhedsNummer', 'cvrNummer', 'enhedstype',
                       'dataAdgang', 'brancheAnsvarskode', 'fejlBeskrivelse',
                       'fejlRegistreret', 'fejlVedIndlaesning',
                       'reklamebeskyttet', 'samtId', 'virkningsAktoer']
        table = alchemy_tables.Virksomhed
        table_columns = [table.enhedsnummer, table.cvrnummer, table.enhedstype, table.dataadgang,
                         table.brancheansvarskode, table.fejlbeskrivelse, table.fejlregistreret,
                         table.fejlvedindlaesning, table.reklamebeskyttet, table.samtid, table.virkningsaktoer,
                         table.naermestefremtidigedato, table.sidstindlaest, table.sidstopdateret]
        return fp.StaticParser(table_class=table,  json_fields=json_fields, json_timestamps=timestamps,
                               table_columns=table_columns)

    def get_value_parser(self):
        """ Get the parsers for cvr values that can be read out without a previous pass,
        livsforloeb, attributter, and for collecting values.
        Dynamic field Values to get: branchekode, navn, binavne, virksomhedsform, virksomhedsstatus, status, 
        regnummer, elektroniskpost, telefon, telefax, obligatoriskemail, hjemmeside        
        """
        vp = fp.ParserList()
        # virksomhedsform
        virksomhedsform_config = {
            'table_class': alchemy_tables.Virksomhedsform,
            'json_fields': ['virksomhedsform'],
            'key': 'virksomhedsformkode',
            'data_fields': ['virksomhedsformkode',
                            'kortBeskrivelse',
                            'langBeskrivelse',
                            'ansvarligDataleverandoer'],
            'columns': [alchemy_tables.Virksomhedsform.virksomhedsformkode,
                        alchemy_tables.Virksomhedsform.kortbeskrivelse,
                        alchemy_tables.Virksomhedsform.langbeskrivelse,
                        alchemy_tables.Virksomhedsform.ansvarligdataleverandoer],
            'mapping': self.key_store.get_virksomhedsform_mapping()
        }
        # virksomhedsstatus
        virksomhedsstatus_config = {
            'table_class': alchemy_tables.Virksomhedsstatus,
            'json_fields': ['virksomhedsstatus'],
            'key': 'status',
            'data_fields': ['status'],
            'columns': [alchemy_tables.Virksomhedsstatus.virksomhedsstatus],
            'mapping': self.key_store.get_virksomhedsstatus_mapping()
        }
        # regnummer
        regnummer_config = {
            'table_class': alchemy_tables.Regnummer,
            'json_fields': ['regNummer'],
            'key': 'regnummer',
            'data_fields': ['regnummer'],
            'columns': [alchemy_tables.Regnummer.regnummer],
            'mapping': self.key_store.get_regnummer_mapping()
        }
        vp.add_listener(fp.ParserFactory.get_branche_parser(self.key_store))
        vp.add_listener(fp.ParserFactory.get_navne_parser(self.key_store))
        vp.add_listener(fp.ParserFactory.get_kontakt_parser(self.key_store))
        configs = [virksomhedsform_config,
                   virksomhedsstatus_config,
                   regnummer_config]
        for config in configs:
            vp.add_listener(fp.UploadData(**config))
        # status  needs double key
        vp.add_listener(UploadStatusTyper(key_store=self.key_store.get_status_mapping()))
        # livsforloeb - move to dyna parser
        vp.add_listener(fp.UploadLivsforloeb())
        # attributter - move to dyna parser
        vp.add_listener(fp.AttributParser())
        # employment - move to dyna parser
        [vp.add_listener(x) for x in fp.get_employment_list()]

        return vp
    
    def get_dyna_parser(self):
        """ Creates data parsing objects for dynamic comapny cvr data fields """
        vp = fp.ParserList()
        vp.add_listener(StatusKoderMap())

        ### Direct Inserts
        virksomhedsform = fp.UpdateMapping(json_field='virksomhedsform', key='virksomhedsformkode', field_type='virksomhedsform')
        penheder = fp.UpdateMapping(json_field='penheder', key='pNummer', field_type='penhed')

        ### Mapped Inserts
        # company status
        virksomhedsstatus_mapping = self.key_store.get_virksomhedsstatus_mapping()
        virksomhedsstatus = fp.UpdateMapping(json_field='virksomhedsstatus', key='status', field_type='virksomhedsstatus', field_map=virksomhedsstatus_mapping)
        # regnummer is obsolete i think...
        regnummer_mapping = self.key_store.get_regnummer_mapping()
        regnummer = fp.UpdateMapping(json_field='regNummer', key='regnummer', field_type='regnummer', field_map=regnummer_mapping)
        # # navn, binavn
        name_mapping = self.key_store.get_name_mapping()
        navne = fp.UpdateMapping(json_field='navne', key='navn', field_type='navn', field_map=name_mapping)
        binavne = fp.UpdateMapping(json_field='binavne', key='navn', field_type='binavn', field_map=name_mapping)
        # kontaktinfo
        kontakt_mapping = self.key_store.get_kontakt_mapping()
        # elektroniskpost
        epost = fp.UpdateMapping(json_field='elektroniskPost', key='kontaktoplysning', field_type='elektroniskpost', field_map=kontakt_mapping)

        # telefonnummer
        tlf = fp.UpdateMapping(json_field='telefonNummer', key='kontaktoplysning', field_type='telefonnummer', field_map=kontakt_mapping)
        stlf = fp.UpdateMapping(json_field='sekundaertTelefonNummer', key='kontaktoplysning', field_type='sekundaerttelefonNummer', field_map=kontakt_mapping)

        # telefaxnummer
        fax = fp.UpdateMapping(json_field='telefaxNummer', key='kontaktoplysning', field_type='telefaxnummer', field_map=kontakt_mapping)
        sfax = fp.UpdateMapping(json_field='sekundaertTelefaxNummer', key='kontaktoplysning', field_type='sekundaerttelefaxnummer', field_map=kontakt_mapping)
        
        # obligatoriskemail
        email = fp.UpdateMapping(json_field='obligatoriskEmail', key='kontaktoplysning', field_type='obligatoriskemail', field_map=kontakt_mapping)
        # hjemmeside
        hjemmeside = fp.UpdateMapping(json_field='hjemmeside', key='kontaktoplysning', field_type='hjemmeside', field_map=kontakt_mapping)  

        # Brancher
        branche_mapping = self.key_store.get_branche_mapping()
        hovedbranche = fp.UpdateMapping(json_field='hovedbranche', key=('branchekode', 'branchetekst'),
                                        field_type='hovedbranche', field_map=branche_mapping)
        bibranche1 = fp.UpdateMapping(json_field='bibranche1', key=('branchekode', 'branchetekst'),
                                        field_type='bibranche1', field_map=branche_mapping)
        bibranche2 = fp.UpdateMapping(json_field='bibranche2', key=('branchekode', 'branchetekst'),
                                        field_type='bibranche2', field_map=branche_mapping)
        bibranche3 = fp.UpdateMapping(json_field='bibranche3', key=('branchekode', 'branchetekst'),
                                        field_type='bibranche3', field_map=branche_mapping)

        update_parser = fp.UploadMappedUpdates()
        #for item in (virksomhedsform,  penheder):
        #    update_parser.add_mapping(fp.UpdateMapping(*item))
        for item in (hovedbranche, bibranche1, bibranche2, bibranche3, virksomhedsform, penheder, virksomhedsstatus, regnummer, navne, binavne, epost, 
                    tlf, fax, email, hjemmeside):
            update_parser.add_mapping(item)

        vp.add_listener(update_parser)
        # navne_parser = parser_organisation.OrganisationNavnParser()
        vp.add_listener(parser_organisation.CompanyOrganisationParser())
        vp.add_listener(parser_organisation.CompanyOrganisationMemberParser())
        vp.add_listener(parser_organisation.SpaltningFusionParser())

        return vp


