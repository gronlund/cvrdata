from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Executable, ClauseElement
from sqlalchemy import select
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


def create_view(name, select_stmt):
    engine.execute(CreateView(name, select_stmt))


def create_views():
    """ create all defined views

    """
    create_branche_view()
    create_relation_view()
    create_virk_production_view()
    create_virksomhedsform_view()


def create_branche_view():
    # branche view
    branche = alchemy_tables.Branche
    upd = alchemy_tables.Update
    vs = alchemy_tables.Virksomhed
    virk_branche_query = select([upd.enhedsnummer, vs.cvrnummer, upd.kode.label('branchekode'), branche.branchetekst,
                                 upd.gyldigfra, upd.gyldigtil, upd.sidstopdateret]).\
        where(branche.branchekode == upd.kode).\
        where(upd.enhedsnummer == vs.enhedsnummer).\
        where(upd.felttype=='hovedbranche')
    create_branche = CreateView('virk_hovedbranche', virk_branche_query)
    engine.execute(create_branche)


def create_relation_view():
    org = alchemy_tables.Organisation
    er = alchemy_tables.Enhedsrelation
    virk_relation_query = select([er.updateid, org.hovedtype, org.navn.label('orgnavn'), er.enhedsnummer_deltager,
                                  er.enhedsnummer_virksomhed, er.enhedsnummer_organisation, er.sekvensnr,
                                  er.vaerdinavn, er.vaerdi, er.gyldigfra, er.gyldigtil, er.sidstopdateret]).\
                                  where(er.enhedsnummer_organisation == org.enhedsnummer)
    create_relations = CreateView('all_relations', virk_relation_query)
    engine.execute(create_relations)


def create_virksomhedsform_view():
    upd = alchemy_tables.Update
    vf = alchemy_tables.Virksomhedsform
    vs = alchemy_tables.Virksomhed
    query = select([upd.enhedsnummer, vs.cvrnummer, upd.kode.label('formkode'), vf.kortbeskrivelse, vf.langbeskrivelse,
                    vf.ansvarligdataleverandoer, upd.gyldigfra, upd.gyldigtil, upd.sidstopdateret]).\
                    where(upd.enhedsnummer==vs.enhedsnummer).\
                    where(upd.felttype=='virksomhedsform').\
                    where(upd.kode==vf.virksomhedsformkode)
    create_form = CreateView('virk_virksomhedsform', query)
    engine.execute(create_form)

def create_virk_production_view():
    upd = alchemy_tables.Update
    vs = alchemy_tables.Virksomhed
    query = select([upd.enhedsnummer, vs.cvrnummer, upd.kode.label('punit'), upd.gyldigfra,
                    upd.gyldigtil, upd.sidstopdateret]).\
                    where(upd.enhedsnummer==vs.enhedsnummer).\
                    where(upd.felttype=='penhed')
    create_punit = CreateView('virk_punits', query)
    engine.execute(create_punit)

def create_virk_status_view():
    upd = alchemy_tables.Update
    vs = alchemy_tables.Virksomhedsstatus
    query = select([upd.enhedsnummer, vs.cvrnummer, upd.kode.label('virkstatus'), upd.gyldigfra,
                    upd.gyldigtil, upd.sidstopdateret]).\
                    where(upd.enhedsnummer==vs.enhedsnummer).\
                    where(upd.felttype=='vir')
    create_punit = CreateView('virk_punits', query)
    engine.execute(create_punit)


