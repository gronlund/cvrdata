from . import field_parser as fp
from . import alchemy_tables
# from . import parser_organisation


class PersonParserFactory(object):
    """ Simple Factory for constructing parsers for cvr person information """
    def __init__(self, key_store):
        self.key_store = key_store

    def get_static_parser(self):
        """
        Return parser for person static data fields
        forretningsnoegle, statusKode, stilling, fejlBeskrivelse, fejlRegistreret,
        fejlVedIndlaesning, reklamebeskyttet, samtId,
        """
        json_fields = ['enhedsNummer', 'dataAdgang', 'forretningsnoegle', 'statusKode', 'stilling',
                       'fejlBeskrivelse', 'fejlRegistreret', 'fejlVedIndlaesning', 'samtId', 'enhedstype']
        json_timestamps = ['naermesteFremtidigeDato', 'sidstIndlaest', 'sidstOpdateret']
        table = alchemy_tables.Person
        table_columns = [table.enhedsnummer, table.dataadgang, table.forretningsnoegle, table.statuskode,
                         table.stilling, table.fejlbeskrivelse, table.fejlregistreret, table.fejlvedindlaesning,
                         table.samtid,  table.enhedstype, table.naermestefremtidigedato, table.sidstindlaest,
                         table.sidstopdateret]
        return fp.StaticParser(table_class=table, json_fields=json_fields, json_timestamps=json_timestamps,
                               table_columns=table_columns)
    
    def get_value_parser(self):
        vp = fp.ParserList()
        # navn, kontakt, attributter
        vp.add_listener(fp.ParserFactory.get_navne_parser(self.key_store))
        vp.add_listener(fp.ParserFactory.get_kontakt_parser(self.key_store))
        vp.add_listener(fp.AttributParser())
        return vp
    
    def get_dyna_parser(self):
        vp = fp.ParserList()
        name_mapping = self.key_store.get_name_mapping()
        navne = ('navne', 'navn', 'navn', name_mapping)
        # # kontaktinfo
        contact_mapping = self.key_store.get_kontakt_mapping()
        epost = ('elektroniskPost', 'kontaktoplysning', 'elektroniskpost', contact_mapping)
        tlf = ('telefonNummer', 'kontaktoplysning', 'telefonnummer', contact_mapping)
        fax = ('telefaxNummer', 'kontaktoplysning', 'telefaxnummer', contact_mapping)

        UpdateParser = fp.UploadMappedUpdates()
        for item in [navne, epost, tlf, fax]:
            UpdateParser.add_mapping(fp.UpdateMapping(*item))
        vp.add_listener(UpdateParser)
        # vp.add_listener(orgparser.PersonOrganisationParser(self.dbmodel))

        ## Old Code
        # name_mapping = self.key_store.get_name_mapping()
        # vp.add_listener(fp.UploadTimeMap('navne', 'navn', 'navn', name_mapping))
        # # kontaktinfo
        # contact_mapping = self.key_store.get_kontakt_mapping()
        # vp.add_listener(fp.UploadTimeMap('elektroniskPost', 'kontaktoplysning', 'elektroniskpost', contact_mapping))
        # vp.add_listener(fp.UploadTimeMap('telefonNummer', 'kontaktoplysning', 'telefonnummer', contact_mapping))
        # vp.add_listener(fp.UploadTimeMap('telefaxNummer', 'kontaktoplysning', 'telefaxnummer', contact_mapping))
        # relation parser

        return vp
