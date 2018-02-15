#!/usr/bin/env python3
from . import field_parser as fp
from . import parser_organisation
from . import alchemy_tables
from . import create_session
from .bug_report import add_error


class UploadStatusTyper(fp.Parser):
    """ Simple class for handling 2D keys for status typer """
    def __init__(self, key_store):
        table_class = alchemy_tables.Statuskode
        columns = [alchemy_tables.Statuskode.statuskode, alchemy_tables.Statuskode.kreditoplysningskode]
        super().__init__(table_class=table_class, columns=columns, keystore=key_store)
        self.keystore = key_store

    def insert(self, data):
        for z in data['status']:
            key = (z['statuskode'], z['kreditoplysningkode'])
            if key in self.keystore or key is None:
                continue
            self.keystore.add(key)
            dat = (z['statuskode'], z['kreditoplysningkode'])
            self.db.insert((key, dat))


class StatusKoderMap(fp.Parser):
    """ Simple class for parsing statuskoder that is pairs of numbers (statuskode, kreditoplysningskode """

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
                print('what the fuck', val)
                add_error('Statuskode', data)
                continue
            dat = self.field_map[val]
            tfrom, tto, utc_sidstopdaret = fp.get_date(z)
            dat = (enh, self.field_type, dat, tfrom, tto)
            self.db.insert(dat)


class VirksomhedParserFactory(object):
    def __init__(self, key_store):
        self.key_store = key_store
        
    def get_static_parser(self):
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
        # livsforloeb
        vp.add_listener(fp.UploadLivsforloeb())
        # attributter
        vp.add_listener(fp.AttributParser())
        # employment
        vp.add_listener(fp.get_upload_employment_year())
        vp.add_listener(fp.get_upload_employment_quarter())
        vp.add_listener(fp.get_upload_employment_month())
        return vp
    
    def get_dyna_parser(self):
        """ Creates data parsing objects for dynamic comapny cvr data fields """
        vp = fp.ParserList()
        vp.add_listener(StatusKoderMap())

        # Direct Inserts
        virksomhedsform = ('virksomhedsform', 'virksomhedsformkode', 'virksomhedsform')
        hovedbranche = ('hovedbranche', 'branchekode','hovedbranche')
        bibranche1 = ('bibranche1', 'branchekode', 'bibranche1')
        bibranche2 =  ('bibranche2', 'branchekode', 'bibranche2')
        bibranche3 = ('bibranche3', 'branchekode', 'bibranche3')
        penheder = ('penheder', 'pNummer', 'penhed')

        # Mapped Inserts
        virksomhedsstatus_mapping = self.key_store.get_virksomhedsstatus_mapping()
        virksomhedsstatus = ('virksomhedsstatus', 'status', 'virksomhedsstatus', virksomhedsstatus_mapping)

        regnummer_mapping = self.key_store.get_regnummer_mapping()
        regnummer = ('regNummer', 'regnummer', 'regnummer', regnummer_mapping)
        # # navn, binavn
        name_mapping = self.key_store.get_name_mapping()
        navne = ('navne', 'navn', 'navn', name_mapping)
        binavne = ('binavne', 'navn', 'binavn', name_mapping)
        # kontaktinfo
        kontakt_mapping = self.key_store.get_kontakt_mapping()
        # elektroniskpost
        epost = ('elektroniskPost', 'kontaktoplysning', 'elektroniskpost', kontakt_mapping)
        # telefonnummer
        tlf = ('telefonNummer', 'kontaktoplysning', 'telefonnummer', kontakt_mapping)
        # telefaxnummer
        fax = ('telefaxNummer', 'kontaktoplysning', 'telefaxnummer', kontakt_mapping)
        # # obligatoriskkemail
        email = ('obligatoriskEmail', 'kontaktoplysning', 'obligatoriskemail', kontakt_mapping)
        # # hjemmeside
        hjemmeside = ('hjemmeside', 'kontaktoplysning', 'hjemmeside', kontakt_mapping)

        UpdateParser = fp.UploadMappedUpdates()
        for item in (virksomhedsform, hovedbranche, bibranche1, bibranche2, bibranche3, penheder):
            UpdateParser.add_mapping(fp.UpdateMapping(*item))
        for item in (virksomhedsstatus, regnummer, navne, binavne, epost, tlf, fax, email, hjemmeside):
            UpdateParser.add_mapping(fp.UpdateMapping(*item))

        vp.add_listener(UpdateParser)
        vp.add_listener(parser_organisation.CompanyOrganisationParser())
        vp.add_listener(parser_organisation.SpaltningFusionParser())

        # produktionsenheder
        # vp.add_listener(fp.UploadTimeDirect('penheder', 'pNummer', 'penhed'))
        # # virksomhedsform
        # vp.add_listener(fp.UploadTimeDirect('virksomhedsform',
        #                                     'virksomhedsformkode',
        #                                     'virksomhedsform'))
        # # brancher
        # vp.add_listener(fp.UploadTimeDirect('hovedbranche',
        #                                     'branchekode',
        #                                     'hovedbranche'))
        # vp.add_listener(fp.UploadTimeDirect('bibranche1',
        #                                     'branchekode',
        #                                     'bibranche1'))
        # vp.add_listener(fp.UploadTimeDirect('bibranche2',
        #                                     'branchekode',
        #                                     'bibranche2'))
        # vp.add_listener(fp.UploadTimeDirect('bibranche3',
        #                                     'branchekode',
        #                                     'bibranche3'))
        # vp.add_listener(StatusKoderMap())
        # # virksomhedsstatus
        # virksomhedsstatus_mapping = self.key_store.get_virksomhedsstatus_mapping()
        # vp.add_listener(fp.UploadTimeMap('virksomhedsstatus',
        #                                  'status',
        #                                  'virksomhedsstatus',
        #                                  virksomhedsstatus_mapping))
        # # regnummer
        # regnummer_mapping = self.key_store.get_regnummer_mapping()
        # vp.add_listener(fp.UploadTimeMap('regNummer', 'regnummer', 'regnummer', regnummer_mapping))
        # # navn, binavn
        # name_mapping = self.key_store.get_name_mapping()
        # vp.add_listener(fp.UploadTimeMap('navne', 'navn', 'navn', name_mapping))
        # vp.add_listener(fp.UploadTimeMap('binavne', 'navn', 'binavn', name_mapping))
        # # kontaktinfo
        # kontakt_mapping = self.key_store.get_kontakt_mapping()
        # # elektroniskpost
        # vp.add_listener(fp.UploadTimeMap('elektroniskPost', 'kontaktoplysning', 'elektroniskpost', kontakt_mapping))
        # # telefonnummer
        # vp.add_listener(fp.UploadTimeMap('telefonNummer', 'kontaktoplysning', 'telefonnummer', kontakt_mapping))
        # # telefaxnummer
        # vp.add_listener(fp.UploadTimeMap('telefaxNummer', 'kontaktoplysning', 'telefaxnummer', kontakt_mapping))
        # # obligatoriskkemail
        # vp.add_listener(fp.UploadTimeMap('obligatoriskEmail', 'kontaktoplysning', 'obligatoriskemail', kontakt_mapping))
        # # hjemmeside
        # vp.add_listener(fp.UploadTimeMap('hjemmeside', 'kontaktoplysning', 'hjemmeside', kontakt_mapping))
        # relation parser
        # vp.add_listener(fp.UploadTimeDirect('penheder', 'pNummer', 'penhed'))
        # # virksomhedsform

        return vp

