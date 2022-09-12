# coding: utf-8
from sqlalchemy import (BigInteger, Column, DateTime,
                        Enum, Float, Index, Integer,
                        SmallInteger, String, text, Text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from collections import namedtuple
from . import engine as proxy_engine

Base = declarative_base()
metadata = Base.metadata
default_start_date = text("'1900-01-01 00:00:00'")
default_end_date = text("'2200-01-01 00:00:00'")


class DBModel(object):
    def __init__(self):
        self.proxy_engine = proxy_engine
        metadata.reflect(bind=proxy_engine.get(), views=True)
        #metadata.reflect(bind=proxy_engine)
        Base = automap_base(metadata=metadata)
        Base.prepare()
        #self.proxy_engine = proxy_engine
        self.tables = namedtuple('tables', metadata.tables.keys())(*metadata.tables.values())
        self.tables_dict = self.tables._asdict()
        self.classes = Base.classes


class Aarsbeskaeftigelse(Base):
    __tablename__ = 'Aarsbeskaeftigelse'

    enhedsnummer = Column(BigInteger, primary_key=True, nullable=False)
    aar = Column(Integer, primary_key=True, nullable=False)
    aarsvaerk = Column(Integer)
    ansatte = Column(Integer)
    ansatteinklusivejere = Column(Integer)
    aarsvaerkinterval = Column(String(40, 'utf8mb4_bin'))
    ansatteinterval = Column(String(40, 'utf8mb4_bin'))
    ansatteinklusivejereinterval = Column(String(40, 'utf8mb4_bin'))
    sidstopdateret = Column(DateTime, nullable=True)


class Kvartalsbeskaeftigelse(Base):
    __tablename__ = 'Kvartalsbeskaeftigelse'
    enhedsnummer = Column(BigInteger, primary_key=True, nullable=False)
    aar = Column(Integer, primary_key=True, nullable=False)
    kvartal = Column(Integer, primary_key=True, nullable=False)
    aarsvaerk = Column(Integer)
    ansatte = Column(Integer)
    aarsvaerkinterval = Column(String(40, 'utf8mb4_bin'))
    ansatteinterval = Column(String(40, 'utf8mb4_bin'))
    sidstopdateret = Column(DateTime, nullable=True)

    
class Maanedsbeskaeftigelse(Base):
    __tablename__ = 'Maanedsbeskaeftigelse'
    enhedsnummer = Column(BigInteger, primary_key=True, nullable=False)
    aar = Column(Integer, primary_key=True, nullable=False)
    maaned = Column(Integer, primary_key=True, nullable=False)
    aarsvaerk = Column(Integer)
    ansatte = Column(Integer)
    aarsvaerkinterval = Column(String(40, 'utf8mb4_bin'))
    ansatteinterval = Column(String(40, 'utf8mb4_bin'))
    sidstopdateret = Column(DateTime, nullable=True)

class erstAarsbeskaeftigelse(Base):
    __tablename__ = 'erstAarsbeskaeftigelse'

    enhedsnummer = Column(BigInteger, primary_key=True, nullable=False)
    aar = Column(Integer, primary_key=True, nullable=False)
    aarsvaerk = Column(Integer)
    ansatte = Column(Integer)
    ansatteinklusivejere = Column(Integer)
    aarsvaerkinterval = Column(String(40, 'utf8mb4_bin'))
    ansatteinterval = Column(String(40, 'utf8mb4_bin'))
    ansatteinklusivejereinterval = Column(String(40, 'utf8mb4_bin'))
    sidstopdateret = Column(DateTime, nullable=True)


class erstKvartalsbeskaeftigelse(Base):
    __tablename__ = 'erstKvartalsbeskaeftigelse'
    enhedsnummer = Column(BigInteger, primary_key=True, nullable=False)
    aar = Column(Integer, primary_key=True, nullable=False)
    kvartal = Column(Integer, primary_key=True, nullable=False)
    aarsvaerk = Column(Integer)
    ansatte = Column(Integer)
    aarsvaerkinterval = Column(String(40, 'utf8mb4_bin'))
    ansatteinterval = Column(String(40, 'utf8mb4_bin'))
    sidstopdateret = Column(DateTime, nullable=True)

    
class erstMaanedsbeskaeftigelse(Base):
    __tablename__ = 'erstMaanedsbeskaeftigelse'
    enhedsnummer = Column(BigInteger, primary_key=True, nullable=False)
    aar = Column(Integer, primary_key=True, nullable=False)
    maaned = Column(Integer, primary_key=True, nullable=False)
    aarsvaerk = Column(Integer)
    ansatte = Column(Integer)
    aarsvaerkinterval = Column(String(40, 'utf8mb4_bin'))
    ansatteinterval = Column(String(40, 'utf8mb4_bin'))
    sidstopdateret = Column(DateTime, nullable=True)



class AdresseDawa(Base):
    __tablename__ = 'AdresseDawa'

    # adresseid = Column(Integer, primary_key=True)
    id = Column(String(40, 'utf8mb4_bin'), nullable=False, primary_key=True)
    status = Column(Integer)
    oprettet = Column(DateTime)
    ændret = Column(DateTime)
    vejkode = Column(SmallInteger)
    vejnavn = Column(String(50, 'utf8mb4_bin'))
    adresseringsvejnavn = Column(String(50, 'utf8mb4_bin'))
    husnr = Column(String(5, 'utf8mb4_bin'))
    etage = Column(String(2, 'utf8mb4_bin'))
    dør = Column(String(6, 'utf8mb4_bin'))
    supplerendebynavn = Column(String(50, 'utf8mb4_bin'))
    postnr = Column(SmallInteger)
    postnrnavn = Column(String(50, 'utf8mb4_bin'))
    stormodtagerpostnr = Column(SmallInteger)
    stormodtagerpostnrnavn = Column(String(50, 'utf8mb4_bin'))
    kommunekode = Column(SmallInteger)
    kommunenavn = Column(String(50, 'utf8mb4_bin'))
    ejerlavkode = Column(Integer)
    ejerlavnavn = Column(String(50, 'utf8mb4_bin'))
    matrikelnr = Column(String(10, 'utf8mb4_bin'))
    esrejendomsnr = Column(Integer)
    etrs89koordinat_øst = Column(Float)
    etrs89koordinat_nord = Column(Float)
    wgs84koordinat_bredde = Column(Float)
    wgs84koordinat_længde = Column(Float)
    nøjagtighed = Column(String(2, 'utf8mb4_bin'))
    kilde = Column(Integer)
    tekniskstandard = Column(String(2, 'utf8mb4_bin'))
    tekstretning = Column(Float)
    ddkn_m100 = Column(String(20, 'utf8mb4_bin'))
    ddkn_km1 = Column(String(20, 'utf8mb4_bin'))
    ddkn_km10 = Column(String(20, 'utf8mb4_bin'))
    adressepunktændringsdato = Column(DateTime)
    adgangsadresseid = Column(String(40, 'utf8mb4_bin'))
    adgangsadresse_status = Column(String(1, 'utf8mb4_bin'))
    adgangsadresse_oprettet = Column(DateTime)
    adgangsadresse_ændret = Column(DateTime)
    regionskode = Column(SmallInteger)
    regionsnavn = Column(String(50, 'utf8mb4_bin'))
    jordstykke_ejerlavnavn = Column(String(100, 'utf8mb4_bin'))
    kvhx = Column(String(20, 'utf8mb4_bin'))
    sognekode = Column(SmallInteger)
    sognenavn = Column(String(50, 'utf8mb4_bin'))
    politikredskode = Column(SmallInteger)
    politikredsnavn = Column(String(100, 'utf8mb4_bin'))
    retskredskode = Column(SmallInteger)
    retskredsnavn = Column(String(100, 'utf8mb4_bin'))
    opstillingskredskode = Column(SmallInteger)
    opstillingskredsnavn = Column(String(100, 'utf8mb4_bin'))
    zone = Column(String(40, 'utf8mb4_bin'))
    jordstykke_ejerlavkode = Column(Integer)
    jordstykke_matrikelnr = Column(String(10, 'utf8mb4_bin'))
    jordstykke_esrejendomsnr = Column(Integer)
    kvh = Column(String(20, 'utf8mb4_bin'))
    højde = Column(Float)
    adgangspunktid = Column(String(40, 'utf8mb4_bin'))


class Adresseupdate(Base):
    __tablename__ = 'Adresseupdates'

    updateid = Column(BigInteger, primary_key=True)
    enhedsnummer = Column(BigInteger, nullable=False)
    adressetype = Column(Enum('beliggenhedsadresse', 'postadresse'))
    adressematch = Column(String(128, 'utf8mb4_bin'), nullable=False)
    # kode = Column(BigInteger, nullable=False)
    dawaid = Column(String(40, 'utf8mb4_bin'), nullable=True)
    gyldigfra = Column(DateTime, nullable=False,
                       server_default=default_start_date)
    gyldigtil = Column(DateTime, nullable=False,
                       server_default=default_end_date)
    post_string = Column(String(512, 'utf8mb4_bin'))
    sidstopdateret = Column(DateTime, nullable=True)


class Attributter(Base):
    __tablename__ = 'Attributter'
    updateid = Column(Integer, primary_key=True)
    enhedsnummer = Column(BigInteger)
    sekvensnr = Column(Integer)
    vaerdinavn = Column(String(128, 'utf8mb4_bin'))
    vaerditype = Column(String(32, 'utf8mb4_bin'))
    vaerdi = Column(Text(2**32-1, collation='utf8mb4_bin'))
    gyldigfra = Column(DateTime, nullable=False, server_default=default_start_date)
    gyldigtil = Column(DateTime, nullable=False, server_default=default_end_date)
    sidstopdateret = Column(DateTime, nullable=True)


# class Branche(Base):
#     __tablename__ = 'Branche'
#     branchekode = Column(Integer, primary_key=True)
#     branchetekst = Column(String(255, 'utf8mb4_bin'))

class Branche(Base):
    __tablename__ = 'Branche'
    __table_args__ = (
        Index('branche_index',
              'branchekode',
              'branchetekst',
              unique=True),
    )
    brancheid = Column(Integer, primary_key=True)
    branchekode = Column(Integer, nullable=False)
    branchetekst = Column(String(160, 'utf8mb4_bin'))


class Enhedsrelation(Base):
    __tablename__ = 'Enhedsrelation'
    updateid = Column(BigInteger, primary_key=True)
    enhedsnummer_deltager = Column(BigInteger, nullable=False)
    enhedsnummer_virksomhed = Column(BigInteger, nullable=False)
    enhedsnummer_organisation = Column(BigInteger, nullable=False)
    sekvensnr = Column(Integer, nullable=False)
    vaerdinavn = Column(String(256, 'utf8mb4_bin'), nullable=False)
    vaerdi = Column(String(2**11, 'utf8mb4_bin'))
    gyldigfra = Column(DateTime, nullable=False, server_default=default_start_date)
    gyldigtil = Column(DateTime, nullable=False, server_default=default_end_date)
    sidstopdateret = Column(DateTime, nullable=True)


class Kontaktinfo(Base):
    __tablename__ = 'Kontaktinfo'
    oplysningid = Column(Integer, primary_key=True)
    kontaktoplysning = Column(String(768, 'utf8mb4_bin'), nullable=False, unique=True)




class Lastupdated(Base):
    __tablename__ = "LastUpdated"
    updatetype = Column(String(255, 'utf8mb4_bin'), primary_key=True)
    lastupdated = Column(DateTime, nullable=False)


class Livsforloeb(Base):
    __tablename__ = 'Livsforloeb'
    __table_args__ = (
        Index('livsforleb_enheds_index',
              'enhedsnummer',
              'gyldigfra',
              'gyldigtil',
              unique=True),
    )
    updateid = Column(BigInteger, primary_key=True)
    enhedsnummer = Column(BigInteger, nullable=False)
    gyldigfra = Column(DateTime, nullable=False,
                       server_default=default_start_date)
    gyldigtil = Column(DateTime, nullable=False,
                       server_default=default_end_date)
    sidstopdateret = Column(DateTime, nullable=True)


class Navne(Base):
    __tablename__ = 'Navne'
    navnid = Column(Integer, primary_key=True)
    navn = Column(String(1024, 'utf8_bin'), nullable=False, unique=True)


class Organisation(Base):
    __tablename__ = 'Organisation'
    updateid = Column(BigInteger, primary_key=True)
    enhedsnummer = Column(BigInteger, nullable=False)
    hovedtype = Column(String(256, 'utf8mb4_bin'), nullable=False)
    navn = Column(String(256, 'utf8mb4_bin'), nullable=False)
    gyldigfra = Column(DateTime, nullable=False,
                       server_default=default_start_date)
    gyldigtil = Column(DateTime, nullable=False,
                       server_default=default_end_date)
    sidstopdateret = Column(DateTime, nullable=True)


class Person(Base):
    __tablename__ = 'Person'
    enhedsnummer = Column(BigInteger, primary_key=True)
    forretningsnoegle = Column(BigInteger)
    statuskode = Column(String(10, 'utf8mb4_bin'))
    stilling = Column(String(255, 'utf8mb4_bin'))
    dataadgang = Column(Integer)
    enhedstype = Column(String(255, 'utf8mb4_bin'))
    fejlbeskrivelse = Column(String(255, 'utf8mb4_bin'))
    fejlregistreret = Column(Integer)
    fejlvedindlaesning = Column(Integer)
    naermestefremtidigedato = Column(DateTime)
    reklamebeskyttet = Column(Integer)
    samtid = Column(Integer)
    sidstindlaest = Column(DateTime)
    sidstopdateret = Column(DateTime)


class Produktion(Base):
    __tablename__ = 'Produktion'
    enhedsnummer = Column(BigInteger, primary_key=True)
    pnummer = Column(BigInteger, index=True)
    enhedstype = Column(String(256, 'utf8mb4_bin'))
    dataadgang = Column(Integer)
    brancheansvarskode = Column(Integer)
    fejlbeskrivelse = Column(String(255, 'utf8mb4_bin'))
    fejlregistreret = Column(Integer)
    fejlvedindlaesning = Column(Integer)
    naermestefremtidigedato = Column(DateTime)
    reklamebeskyttet = Column(Integer)
    samtid = Column(Integer)
    sidstindlaest = Column(DateTime)
    sidstopdateret = Column(DateTime)
    virkningsaktoer = Column(String(64, 'utf8mb4_bin'))


class Regnummer(Base):
    __tablename__ = 'Regnummer'
    regid = Column(Integer, primary_key=True)
    regnummer = Column(String(20, 'utf8mb4_bin'), nullable=False, unique=True)


class SpaltningFusion(Base):
    __tablename__ = 'SpaltningFusion'
    updateid = Column(BigInteger, primary_key=True)
    enhedsnummer = Column(BigInteger, nullable=False)
    enhedsnummer_organisation = Column(BigInteger, nullable=False)
    spalt_fusion = Column(Enum('spaltning', 'fusion'))
    indud = Column(Enum('indgaaende', 'udgaaende'), nullable=False)
    gyldigfra = Column(DateTime, nullable=False,
                       server_default=default_start_date)
    gyldigtil = Column(DateTime, nullable=False,
                       server_default=default_end_date)
    sidstopdateret = Column(DateTime, nullable=True)


class Statuskode(Base):
    __tablename__ = 'Statuskode'
    __table_args__ = (
        Index('status_kode_kombi_index',
              'statuskode',
              'kreditoplysningskode',
              unique=True),
    )
    statusid = Column(Integer, primary_key=True)
    statuskode = Column(Integer, nullable=False)
    statustekst = Column(Text(2**32-1, collation='utf8mb4_bin'), nullable=True)
    kreditoplysningskode = Column(Integer, nullable=False)
    kreditoplysningtekst = Column(Text(2**32-1, collation='utf8mb4_bin'), nullable=True)


class Update(Base):
    __tablename__ = 'Updates'
    updateid = Column(BigInteger, primary_key=True)
    enhedsnummer = Column(BigInteger, nullable=False)
    felttype = Column(String(256, 'utf8mb4_bin'), nullable=False)
    kode = Column(BigInteger, nullable=False)
    gyldigfra = Column(DateTime, nullable=False,
                       server_default=default_start_date)
    gyldigtil = Column(DateTime, nullable=False,
                       server_default=default_end_date)
    sidstopdateret = Column(DateTime, nullable=True)


class Virksomhed(Base):
    __tablename__ = 'Virksomhed'
    enhedsnummer = Column(BigInteger, primary_key=True)
    cvrnummer = Column(Integer, nullable=False, unique=True)
    enhedstype = Column(String(256, 'utf8mb4_bin'))
    dataadgang = Column(Integer)
    brancheansvarskode = Column(Integer)
    fejlbeskrivelse = Column(String(255, 'utf8mb4_bin'))
    fejlregistreret = Column(Integer)
    fejlvedindlaesning = Column(Integer)
    naermestefremtidigedato = Column(DateTime)
    reklamebeskyttet = Column(Integer)
    samtid = Column(Integer)
    sidstindlaest = Column(DateTime)
    sidstopdateret = Column(DateTime)
    virkningsaktoer = Column(String(64, 'utf8mb4_bin'))


class Virksomhedsform(Base):
    __tablename__ = 'Virksomhedsform'
    virksomhedsformkode = Column(Integer, primary_key=True)
    kortbeskrivelse = Column(String(20, 'utf8mb4_bin'))
    langbeskrivelse = Column(String(255, 'utf8mb4_bin'))
    ansvarligdataleverandoer = Column(String(255, 'utf8mb4_bin'))


class Virksomhedsstatus(Base):
    __tablename__ = 'Virksomhedsstatus'
    virksomhedsstatusid = Column(Integer, primary_key=True)
    virksomhedsstatus = Column(String(768, 'utf8mb4_bin'),
                               nullable=False, unique=True)


class Registration(Base):
    __tablename__ = 'Registration'
    registrationid = Column(BigInteger, primary_key=True)
    adresse = Column(String(255, collation='utf8mb4_bin'))
    cvrnummer = Column(Integer)
    hovednavn = Column(String(255, 'utf8mb4_bin'))
    kommunekode = Column(Integer)
    offentliggoerelseid = Column(Integer, unique=True)
    offentliggoerelsetidsstempel = Column(DateTime)
    opdateret = Column(DateTime)
    oprettet =  Column(DateTime)
    postnummer = Column(Integer)
    registreringtidsstempel = Column(DateTime)
    sidstopdateret = Column(DateTime)
    tekst  = Column(Text(2**30-1, collation='utf8mb4_bin'))
    ren_tekst  = Column(Text(2**30-1, collation='utf8mb4_bin'))
    virksomhedsformkode = Column(Integer)
    virksomhedsregistreringstatusser =  Column(String(255, collation='utf8mb4_bin'))
    

class CreateDatabase(object):
    def __init__(self):
        self.cvr_tables = [Aarsbeskaeftigelse,
                           erstAarsbeskaeftigelse,
                           AdresseDawa,
                           Adresseupdate,
                           Attributter,
                           Branche,
                           Enhedsrelation,
                           Kontaktinfo,
                           Kvartalsbeskaeftigelse,
                           erstKvartalsbeskaeftigelse,
                           Livsforloeb,
                           Maanedsbeskaeftigelse,
                           erstMaanedsbeskaeftigelse,
                           Navne,
                           Organisation,
                           Person,
                           Produktion,
                           Registration,
                           Regnummer,
                           SpaltningFusion,
                           Statuskode,
                           Update,
                           Virksomhed,
                           Virksomhedsform,
                           Virksomhedsstatus
                           ]
    
    
    def delete_tables(self):
        pass    

    def create_tables(self):
        # base.metadata.create_all(proxy_engine, tables=[x.__table__ for x in tables])
        print('Create Tables')
        for x in self.cvr_tables:
            try:
                print('Creating Table {0}'.format(x.__tablename__))
                x.__table__.create(proxy_engine)
            except Exception as e:
                print('Create Table Exception: ', x.__tablename__)
                print('Probably already exists')
                print(e)

    def create_query_indexes(self):
        """ Create Indexes used for queries

        :return:
        """
        #attributter_type_index = Index('attributter_type_index', Attributter.vaerdinavn)
        # enheds_vaerdinavn_index = Index('enheds_vaerdinavn_index',
        # Enhedsrelation.vaerdinavn, Enhedsrelation.vaerdi)

        update_type_index = Index('updates_type_index',
                                  Update.felttype,
                                  Update.kode,
                                  Update.enhedsnummer,
                                  Update.gyldigfra,
                                  Update.gyldigtil)
        #spalt_org = Index('spalt_virk_index', SpaltningFusion.enhedsnummer_organisation)
        #org_navn = Index('orgnavn_navn', Organisation.navn)
        #org_hovedtype = Index('orgnavn_hovedtype', Organisation.hovedtype, Organisation.navn)
        enheds_org_index = Index('enheds_org_index',
                                 Enhedsrelation.enhedsnummer_organisation)
        branchekode_index = Index('branchekode_index',
                                 Branche.branchekode
                                 )
        branchetekst_index = Index('branchetekst_index',
                                   Branche.branchetekst
                                   )
        # attributter_type_index,
                         # attributter_value_index,
                         # enheds_vaerdi_index,
                         # enheds_vaerdinavn_index,
                         # org_navn,
                         # org_hovedtype,
                         # spalt_org,
        query_indexes = [enheds_org_index, update_type_index,
                         branchekode_index, branchetekst_index
                         ]
        for index in query_indexes:
            print('Creating index', index.name)
            try:
                index.create(proxy_engine)
            except Exception as e:
                print(e)

        # text_indexes = [(Enhedsrelation, vaerdi)]

    def create_text_indexes(self):
        #enheds_vaerdi_index = Index('enheds_vaerdi_index', Enhedsrelation.vaerdi)  # text index
        #attributter_value_index = Index('attributter_value_index', Attributter.vaerdi)
        pass


    def create_update_indexes(self):
        """ create (unique) indexes of database that are vital
        for fast update (deletion/insert)

        :return:
        """
        print('Create Update Indexes')
        adresse_unique = Index('adresse_time_index',
                               Adresseupdate.enhedsnummer,
                               Adresseupdate.dawaid,
                               Adresseupdate.gyldigfra,
                               Adresseupdate.gyldigtil, unique=True)
        attr_enheds_index = Index('attributter_enhedsummer_index',
                                  Attributter.enhedsnummer,
                                  Attributter.vaerdinavn,
                                  Attributter.sekvensnr,
                                  Attributter.gyldigfra,
                                  Attributter.gyldigtil)
        enheds_deltager = Index('enheds_deltager',
                                Enhedsrelation.enhedsnummer_deltager)
        enheds_virksomhed_index = Index('enheds_virksomhed_index',
                                        Enhedsrelation.enhedsnummer_virksomhed)
        org_unique = Index('orgnavn_unique',
                           Organisation.enhedsnummer,
                           Organisation.hovedtype,
                           Organisation.navn,
                           unique=True)
        spalt_unique = Index('spalt_unique',
                             SpaltningFusion.enhedsnummer,
                             SpaltningFusion.enhedsnummer_organisation,
                             SpaltningFusion.spalt_fusion,
                             SpaltningFusion.indud,
                             SpaltningFusion.gyldigfra,
                             SpaltningFusion.gyldigtil, unique=True)
        update_enhedsnummer_index = Index('updates_unique_index',
                                          Update.enhedsnummer,
                                          Update.felttype,
                                          Update.kode,
                                          Update.gyldigfra,
                                          Update.gyldigtil)

        # enheds_unique = Index('enheds_deltager', Enhedsrelation.enhedsnummer_deltager,
        # Enhedsrelation.enhedsnummer_virksomhed, Enhedsrelation.enhedsnummer_organisation, Enhedsrelation.sekvensnr,
        # Enhedsrelation.vaerdinavn, Enhedsrelation.vaerdi, Enhedsrelation.gyldigfra, Enhedsrelation.gyldigtil)
        # Enhedsrelation.enhedsnummer_deltager, Enhedsrelation.enhedsnummer_organisation,
        # Enhedsrelation.sekvensnr, Enhedsrelation.vaerdinavn,
        # Enhedsrelation.vaerdi, Enhedsrelation.gyldigfra, Enhedsrelation.gyldigtil)

        indexes = [adresse_unique,
                   attr_enheds_index,
                   enheds_virksomhed_index,
                   enheds_deltager,
                   org_unique,
                   spalt_unique,
                   update_enhedsnummer_index
                   ]
        for index in indexes:
            print('Creating index', index.name)
            try:
                index.create(proxy_engine)
            except Exception as e:
                print('Index construction failed', index.name)
                print('Probably already exists')
                print(e)


    def create_my_sql_text_index(self, my_class, full_text_columns):
        mysql_full_text_command = """ALTER TABLE {0.__tablename__} ADD FULLTEXT ({1})"""
        mysql_command = mysql_full_text_command.format(my_class, ", ".join(column for column in full_text_columns))
        proxy_engine.execute(mysql_command)
