from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Executable, ClauseElement
from sqlalchemy import select
from sqlalchemy.schema import DDLElement
from . import alchemy_tables
from . import engine


class CreateView(Executable, ClauseElement):
    """ Create View
    use as
    createview = CreateView('viewname', t.select().where(t.c.id>5))
    engine.execute(createview)
    """

    def __init__(self, name, select_stmt):
        """

        :param name: str, name of view
        :param select_stmt: sqlalchemy.select, sql query view represents
        """
        self.name = name
        self.select = select_stmt


@compiles(CreateView, 'mysql')
def visit_create_view(element, compiler):
    return "CREATE VIEW %s AS %s" % (
         element.name,
         compiler.process(element.select, literal_binds=True)
         )


def create_view(name, select_stmt, db):
    """ create a view

    :param name: str,
    :param select_stmt: sqlalchemy query
    :param db: dbmodel
    :return:
    """
    if name in db.tables_dict.keys():
        print('{0} exists'.format(name))
        return
    print('Create View', name, select_stmt)
    engine.execute(CreateView(name, select_stmt))

class DropView(DDLElement):
    def __init__(self, name):
        self.name = name


@compiles(DropView)
def compile(element, compiler, **kw):
    return "DROP VIEW %s" % (element.name)


def drop_view(name, db):
    if not name in db.tables_dict.keys():
        print('view not there?')
    engine.execute(DropView(name))


def create_views():
    """ create all defined views  """
    db = alchemy_tables.DBModel()
    create_person_name_view(db)
    create_branche_view(db)
    create_bibranche_view(db)
    create_virk_production_view(db)
    create_virksomhedsform_view(db)
    create_virk_status_view(db)
    create_virk_kredit_status_view(db)
    create_virk_name_view(db)
    create_virk_attributter(db)
    create_virk_kontakt_view(db)
    create_relation_view(db)
    create_virk_livsforloeb(db)
    db = alchemy_tables.DBModel()
    create_board_view(db)
    create_direct_owner_view(db)
    create_real_owner_view(db)
    create_stifter_view(db)
    create_revision_view(db)
    create_direktion_view(db)
    create_monthly_employment(db)
    create_quarterly_employment(db)
    create_yearly_employment(db)


def drop_views():
    view_names = ['ejere']
    db = alchemy_tables.DBModel()
    for view_name in view_names:
        drop_view(view_name, db)


def create_branche_view(db):
    """ Create view of main industri code """
    view_name = 'virk_hovedbranche'
    branche = alchemy_tables.Branche
    upd = alchemy_tables.Update
    vs = alchemy_tables.Virksomhed
    virk_branche_query = select([upd.enhedsnummer,
                                 vs.cvrnummer,
                                 branche.brancheid,
                                 branche.branchekode,
                                 branche.branchetekst,
                                 upd.gyldigfra,
                                 upd.gyldigtil,
                                 upd.sidstopdateret]).\
        where(branche.brancheid == upd.kode).\
        where(upd.enhedsnummer == vs.enhedsnummer).\
        where(upd.felttype == 'hovedbranche')
    create_view(view_name, virk_branche_query, db)


def create_bibranche_view(db):
    """ Create view of main industri code """
    view_name = 'virk_branche'
    branche = alchemy_tables.Branche
    upd = alchemy_tables.Update
    vs = alchemy_tables.Virksomhed
    virk_branche_query = select([upd.enhedsnummer,
                                 vs.cvrnummer,
                                 upd.felttype,
                                 branche.brancheid,
                                 branche.branchekode,
                                 branche.branchetekst,
                                 upd.gyldigfra,
                                 upd.gyldigtil,
                                 upd.sidstopdateret]).\
        where(branche.brancheid == upd.kode).\
        where(upd.enhedsnummer == vs.enhedsnummer).\
        where(upd.felttype.in_(['hovedbranche', 'bibranche1', 'bibranche2', 'bibranche3']))
    create_view(view_name, virk_branche_query, db)


def create_virk_kontakt_view(db):
    """ Create view of main industri code """
    view_name = 'virk_kontaktinfo'
    kontakt = alchemy_tables.Kontaktinfo
    upd = alchemy_tables.Update
    vs = alchemy_tables.Virksomhed
    virk_branche_query = select([upd.enhedsnummer,
                                 vs.cvrnummer,
                                 upd.felttype,
                                 upd.kode.label('kontaktid'),
                                 kontakt.kontaktoplysning,
                                 upd.gyldigfra, upd.gyldigtil,
                                 upd.sidstopdateret]).\
        where(kontakt.oplysningid == upd.kode).\
        where(upd.enhedsnummer == vs.enhedsnummer).\
        where(upd.felttype.in_(['elektroniskpost',
                               'hjemmeside',
                               'obligatoriskemail',
                               'telefaxnummer',
                               'telefonnummer']))
    create_view(view_name, virk_branche_query, db)


def create_relation_view(db):
    org = alchemy_tables.Organisation
    er = alchemy_tables.Enhedsrelation
    view_name = 'relationer'
    virk_relation_query = select([er.updateid, org.hovedtype,
                                  org.navn.label('orgnavn'),
                                  er.enhedsnummer_deltager,
                                  er.enhedsnummer_virksomhed,
                                  er.enhedsnummer_organisation,
                                  er.sekvensnr,
                                  er.vaerdinavn, er.vaerdi,
                                  er.gyldigfra,
                                  er.gyldigtil,
                                  er.sidstopdateret
                                  ]).\
        where(er.enhedsnummer_organisation == org.enhedsnummer)
    create_view(view_name, virk_relation_query, db)


def create_revision_view(db):
    org = alchemy_tables.Organisation
    er = alchemy_tables.Enhedsrelation
    view_name = 'revisor'
    query = select([er.updateid, org.hovedtype,
                    org.navn.label('orgnavn'),
                    er.enhedsnummer_deltager.label('revisor'),
                    er.enhedsnummer_virksomhed,
                    er.enhedsnummer_organisation,
                    er.sekvensnr,
                    er.vaerdinavn, er.vaerdi,
                    er.gyldigfra,
                    er.gyldigtil,
                    er.sidstopdateret]).\
        where(er.enhedsnummer_organisation == org.enhedsnummer).\
        where(org.hovedtype == 'REVISION')
    create_view(view_name, query, db)


def create_direktion_view(db):
    org = alchemy_tables.Organisation
    er = alchemy_tables.Enhedsrelation
    view_name = 'direktion'
    query = select([er.updateid, org.hovedtype,
                    org.navn.label('orgnavn'),
                    er.enhedsnummer_deltager,
                    er.enhedsnummer_virksomhed,
                    er.enhedsnummer_organisation,
                    er.sekvensnr,
                    er.vaerdinavn,
                    er.vaerdi,
                    er.gyldigfra,
                    er.gyldigtil,
                    er.sidstopdateret]).\
        where(er.enhedsnummer_organisation == org.enhedsnummer).\
        where(org.navn == 'Direktion')
    create_view(view_name, query, db)


def create_board_view(db):
    """ Create Board Member View """
    view_name = 'bestyrelse'
    rel_table = db.tables.relationer
    cols = rel_table.columns
    board_relation_query = select(cols).\
        where(cols.hovedtype == 'LEDELSESORGAN').\
        where(cols.orgnavn == 'Bestyrelse')
    create_view(view_name, board_relation_query, db)


def create_direct_owner_view(db):
    """ Create view of direct ownerships """
    view_name = 'ejere'
    rel_table = db.tables.relationer
    cols = rel_table.columns
    owner_relation_query = select(cols).where(cols.hovedtype == 'REGISTER').\
        where(cols.orgnavn == 'EJERREGISTER')
    create_view(view_name, owner_relation_query, db)


def create_stifter_view(db):
    """ Create view of direct ownerships """
    view_name = 'stiftere'
    rel_table = db.tables.relationer
    cols = rel_table.columns
    owner_relation_query = select(cols).where(cols.hovedtype == 'STIFTERE')
    create_view(view_name, owner_relation_query, db)


def create_real_owner_view(db):
    """ Create real owner view """
    view_name = 'reelle_ejere'
    rel_table = db.tables.relationer
    cols = rel_table.columns
    real_owner_query = select(cols).where(cols.hovedtype == 'REGISTER').\
        where(cols.orgnavn == 'Reelle ejere')
    create_view(view_name, real_owner_query, db)


def create_virksomhedsform_view(db):
    view_name = 'virk_virksomhedsform'
    upd = alchemy_tables.Update
    vs = alchemy_tables.Virksomhed
    vf = alchemy_tables.Virksomhedsform
    query = select([upd.enhedsnummer,
                    vs.cvrnummer,
                    upd.kode.label('formkode'),
                    vf.kortbeskrivelse,
                    vf.langbeskrivelse,
                    vf.ansvarligdataleverandoer,
                    upd.gyldigfra,
                    upd.gyldigtil,
                    upd.sidstopdateret]).\
        where(upd.enhedsnummer == vs.enhedsnummer).\
        where(upd.felttype == 'virksomhedsform').\
        where(upd.kode == vf.virksomhedsformkode)
    create_view(view_name, query, db)


def create_virk_production_view(db):
    view_name = 'virk_punits'
    upd = alchemy_tables.Update
    vs = alchemy_tables.Virksomhed
    query = select([upd.enhedsnummer,
                    vs.cvrnummer,
                    upd.kode.label('punit'),
                    upd.gyldigfra,
                    upd.gyldigtil,
                    upd.sidstopdateret]).\
        where(upd.enhedsnummer == vs.enhedsnummer).\
        where(upd.felttype == 'penhed')
    create_view(view_name, query, db)


def create_virk_status_view(db):
    upd = alchemy_tables.Update
    vs = alchemy_tables.Virksomhed
    stat = alchemy_tables.Virksomhedsstatus
    query = select([upd.enhedsnummer,
                    vs.cvrnummer,
                    upd.kode.label('virksomhedsstatuskode'),
                    stat.virksomhedsstatus,
                    upd.gyldigfra,
                    upd.gyldigtil,
                    upd.sidstopdateret]).\
        where(upd.enhedsnummer == vs.enhedsnummer).\
        where(upd.felttype == 'virksomhedsstatus').\
        where(stat.virksomhedsstatusid==upd.kode)
    create_view('virk_virksomhedsstatus', query, db)


def create_virk_attributter(db):
    att = alchemy_tables.Attributter
    vs = alchemy_tables.Virksomhed
    query = select([vs.enhedsnummer,
                    vs.cvrnummer,
                    att.vaerdinavn,
                    att.vaerdi,
                    att.vaerditype,
                    att.sekvensnr,
                    att.gyldigfra,
                    att.gyldigtil,
                    att.sidstopdateret]).\
        where(att.enhedsnummer == vs.enhedsnummer)
    create_view('virk_attributter', query, db)


def create_virk_name_view(db):
    view_name = 'virk_navne'
    upd = alchemy_tables.Update
    vs = alchemy_tables.Virksomhed
    navn = alchemy_tables.Navne
    query = select([upd.enhedsnummer,
                    vs.cvrnummer,
                    upd.felttype,
                    upd.kode.label('navnid'),
                    navn.navn,
                    upd.gyldigfra,
                    upd.gyldigtil,
                    upd.sidstopdateret]).\
        where(upd.enhedsnummer == vs.enhedsnummer).\
        where(upd.felttype.in_(['navn', 'binavn'])).\
        where(upd.kode == navn.navnid)
    create_view(view_name, query, db)


def create_virk_livsforloeb(db):
    view_name = 'virk_livsforloeb'
    lvs = alchemy_tables.Livsforloeb
    vs = alchemy_tables.Virksomhed
    query = select([vs.enhedsnummer,
                    vs.cvrnummer,
                    lvs.gyldigfra,
                    lvs.gyldigtil,
                    lvs.sidstopdateret]).\
        where(lvs.enhedsnummer == vs.enhedsnummer)
    create_view(view_name, query, db)

    
def create_monthly_employment(db):
    view_name = 'monthly_employment'
    lvs = alchemy_tables.Maanedsbeskaeftigelse
    vs = alchemy_tables.Virksomhed
    query = select([vs.enhedsnummer,
                    vs.cvrnummer,
                    lvs.aar,
                    lvs.maaned,
                    lvs.aarsvaerk,
                    lvs.ansatte,
                    lvs.ansatteinterval,
                    lvs.aarsvaerkinterval,
                    lvs.sidstopdateret]).\
        where(lvs.enhedsnummer == vs.enhedsnummer)
    create_view(view_name, query, db)


def create_quarterly_employment(db):
    view_name = 'quarterly_employment'
    lvs = alchemy_tables.Kvartalsbeskaeftigelse
    vs = alchemy_tables.Virksomhed
    query = select([vs.enhedsnummer,
                    vs.cvrnummer,
                    lvs.aar,
                    lvs.kvartal,
                    lvs.aarsvaerk,
                    lvs.ansatte,
                    lvs.aarsvaerkinterval,
                    lvs.ansatteinterval,
                    lvs.sidstopdateret]).\
        where(lvs.enhedsnummer == vs.enhedsnummer)
    create_view(view_name, query, db)


def create_yearly_employment(db):
    view_name = 'yearly_employment'
    lvs = alchemy_tables.Aarsbeskaeftigelse
    vs = alchemy_tables.Virksomhed
    query = select([vs.enhedsnummer,
                    vs.cvrnummer,
                    lvs.aar,
                    lvs.aarsvaerk,
                    lvs.ansatte,
                    lvs.ansatteinklusivejere,
                    lvs.aarsvaerkinterval,
                    lvs.ansatteinterval,
                    lvs.ansatteinklusivejereinterval,
                    lvs.sidstopdateret]).\
        where(lvs.enhedsnummer == vs.enhedsnummer)
    create_view(view_name, query, db)


def create_virk_kredit_status_view(db):
    upd = alchemy_tables.Update
    vs = alchemy_tables.Virksomhed
    stat = alchemy_tables.Statuskode
    query = select([upd.enhedsnummer,
                    vs.cvrnummer,
                    upd.kode.label('statuskodeid'),
                    stat.statuskode,
                    stat.statustekst,
                    stat.kreditoplysningskode,
                    stat.kreditoplysningtekst,
                    upd.gyldigfra,
                    upd.gyldigtil,
                    upd.sidstopdateret]).\
        where(upd.enhedsnummer == vs.enhedsnummer).\
        where(upd.felttype == 'status').\
        where(stat.statusid == upd.kode)
    create_view('virk_status', query, db)


def create_person_name_view(db):
    view_name = 'pers_navne'
    upd = alchemy_tables.Update
    vs = alchemy_tables.Person
    navn = alchemy_tables.Navne
    query = select([upd.enhedsnummer,
                    upd.kode.label('navnid'),
                    navn.navn,
                    upd.gyldigfra,
                    upd.gyldigtil,
                    upd.sidstopdateret]).\
        where(upd.enhedsnummer == vs.enhedsnummer).\
        where(upd.felttype == 'navn').\
        where(upd.kode == navn.navnid)
    create_view(view_name, query, db)


