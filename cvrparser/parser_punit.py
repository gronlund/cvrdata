from . import field_parser as fp
from . import parser_organisation
from . import alchemy_tables


class PenhedParserFactory(object):
    """Simple Factory for making parsers for cvr production unit information"""
    def __init__(self, key_store):
        self.key_store = key_store

    def get_static_parser(self):
        """ return parser for static data fields"""
        json_fields = ['enhedsNummer', 'pNummer', 'enhedstype', 'reklamebeskyttet', 'dataAdgang', 'fejlBeskrivelse',
                       'fejlRegistreret', 'fejlVedIndlaesning', 'samtId', 'virkningsaktoer']
        timestamps = ['naermesteFremtidigeDato', 'sidstIndlaest', 'sidstOpdateret']
        table = alchemy_tables.Produktion
        table_columns = [table.enhedsnummer, table.pnummer, table.enhedstype, table.reklamebeskyttet,
                         table.dataadgang, table.fejlbeskrivelse, table.fejlregistreret,
                         table.fejlvedindlaesning, table.samtid, table.virkningsaktoer,
                         table.naermestefremtidigedato, table.sidstindlaest, table.sidstopdateret]
        return fp.StaticParser(table_class=table, json_fields=json_fields,  json_timestamps=timestamps,
                               table_columns=table_columns)

    def get_value_parser(self):
        """ Penhed data parsers that extract the existing values in the data for different 
        information fields and saves them in database 
        """
        vp = fp.ParserList()
        vp.add_listener(fp.ParserFactory.get_branche_parser(self.key_store))
        vp.add_listener(fp.ParserFactory.get_navne_parser(self.key_store))
        vp.add_listener(fp.ParserFactory.get_kontakt_parser(self.key_store))
        # livsforloeb
        vp.add_listener(fp.UploadLivsforloeb())
        # attributter
        vp.add_listener(fp.AttributParser())
        # interval employment
        vp.add_listener(fp.get_upload_employment_year())
        vp.add_listener(fp.get_upload_employment_quarter())
        vp.add_listener(fp.get_upload_employment_month())
        return vp

    def get_dyna_parser(self):
        vp = fp.ParserList()
        vp.add_listener(fp.UploadTimeDirect('hovedbranche', 'branchekode', 'hovedbranche'))
        vp.add_listener(fp.UploadTimeDirect('bibranche1', 'branchekode', 'bibranche1'))
        vp.add_listener(fp.UploadTimeDirect('bibranche2', 'branchekode', 'bibranche2'))
        vp.add_listener(fp.UploadTimeDirect('bibranche3', 'branchekode', 'bibranche3'))
        vp.add_listener(fp.UploadTimeDirect('virksomhedsrelation', 'cvrNummer', 'penhed'))
        # navn, binavn
        navn_mapping = self.key_store.get_name_mapping()
        vp.add_listener(fp.UploadTimeMap('navne', 'navn', 'navn', navn_mapping))
        # kontaktinfo
        kontakt_mapping = self.key_store.get_kontakt_mapping()
        # elektroniskpost
        vp.add_listener(fp.UploadTimeMap('elektroniskPost', 'kontaktoplysning', 'elektroniskpost', kontakt_mapping))
        # telefonnummer
        vp.add_listener(fp.UploadTimeMap('telefonNummer', 'kontaktoplysning', 'telefonnummer', kontakt_mapping))
        # telefaxnummer
        vp.add_listener(fp.UploadTimeMap('telefaxNummer', 'kontaktoplysning', 'telefaxnummer', kontakt_mapping))
        vp.add_listener(parser_organisation.CompanyOrganisationParser())
        return vp
