""" Giant BEAST trying to match old CVR addresses to DAWA addresses"""
import re
import numpy as np
import itertools as it
import os
import requests
import Levenshtein as Le
from .alchemy_tables import AdresseDawa
from . import create_session


def make_adresse_dict():
    """make tree (dict) of dawa adresses of kommunekode, vejkode, husnr, etage, doer, dawaid"""
    session = create_session()
    query = session.query(AdresseDawa.kommunekode, AdresseDawa.vejkode, AdresseDawa.husnr,
                          AdresseDawa.dør, AdresseDawa.id)
    # sql_query = """select kommunekode,vejkode,husnr,etage,dør,id from AdresseDawa
    #                order by kommunekode, vejkode, husnr, etage, dør"""
    res = [(k, v, str_clean(h), str_clean(e), str_clean(d), a) for (k, v, h, e, d, a) in query.all()]
    kmap = gb(res)
    return kmap


def make_post_grupper():
    """ make postnr og navn som kan forveksles ind i grupper
    sql_query = 'select distinct postnr,postnrnavn from AdresseDawa'
    """
    session = create_session()
    query = session.query(AdresseDawa.postnr, AdresseDawa.postnrnavn).distinct()
    dat = [(str_clean(y.split()[0]), x) for (x, y) in query.all()]
    rele = ['København', 'Frederiksberg', 'Odense', 'Randers', 'Esbjerg', 'Aalborg', 'Aarhus',
            'Faxe', 'Vallensbæk', 'Vejle']
    rele = [str_clean(x) for x in rele]
    kv = sorted([x for x in dat if x[0] in rele])
    kg = {}
    for key, val in it.groupby(kv, lambda x: x[0]):
        kg[key] = [x[1] for x in val]
    return kg


def make_postnr_vejnavn_map():
    """ lav maps til postnummer,vejnavn
    sql_query = 'select distinct postnr,postnrnavn,vejnavn,kommunekode,vejkode from AdresseDawa'
    """
    with create_session() as session:
        query = session.query(AdresseDawa.postnr, AdresseDawa.postnrnavn, AdresseDawa.vejnavn, AdresseDawa.kommunekode,
                              AdresseDawa.vejkode).distinct()
        dat = [(x, str_clean(y), str_clean(z), (a, b)) for (x, y, z, a, b) in query.all()]
        pv_map = gb(dat)
        return pv_map


def make_kmap():
    """
    sql_query = 'SELECT distinct kommunenavn,kommunekode from AdresseDawa'
    :return: dict - map from kommunenavn til kode
    """
    # generate map from database
    with create_session() as session:
        query = session.query(AdresseDawa.kommunenavn, AdresseDawa.kommunekode).distinct()
        kmap = {str_clean(x): y for (x, y) in query.all()}
        return kmap


def get_adresse_maps():
    return {
        'adr_tree': make_adresse_dict(),
        'postnr_map': make_postnr_vejnavn_map(),
        'post_grupper': make_post_grupper(),
        'kmap': make_kmap()
    }


def get_husnr(husnummer_fra, bogstav_fra):
    res = ""
    if husnummer_fra is not None:
        res += str(int(husnummer_fra))
    if bogstav_fra is not None:
        res += "{0}".format(bogstav_fra)
    return str_clean(res)


def beliggenhedsadresse_to_str(beliggenhedsadresse):
    """
    @param beliggenhedsadresse is a dict describing an address as per cvr data.
    @return a string of the form <vejnavn> <husnummer>[<bogstav>][ <etage>][, <postnr>]
    """
    #import pdb 
    #pdb.set_trace()
    #assert(type(beliggenhedsadresse) == dict)
    
    vejnavn = beliggenhedsadresse['vejnavn']
    if vejnavn is None or len(vejnavn) == 0:
        return None
    
    husnummer = beliggenhedsadresse['husnummerFra']
    bogstav = beliggenhedsadresse['bogstavFra']
    etage = beliggenhedsadresse['etage']
    postnr = beliggenhedsadresse['postnummer']
    doer = beliggenhedsadresse['sidedoer']
    postdistrikt = beliggenhedsadresse['postdistrikt']
    
    res = vejnavn
    hr = get_husnr(husnummer, bogstav)
    if len(hr) > 0:
        res += ' ' + hr
    if etage is not None and doer is not None:
        res += " %s." % etage
        res += " {0}".format(doer)
    if postnr is not None:
        res += ", %s" % postnr
    if postdistrikt is not None:
        res += " %s" % postdistrikt       
    return res


def clean_adresse(bla):
    """ Cleans and address for weird things and makes all strings lower case and
    removes irrelevant zeros from numbers """
    for k, v in bla.items():
        if isinstance(bla[k], str):  # maybe just is not None
            bla[k] = str_clean(bla[k])
            if bla[k].isdigit():  # mostly only husnr, etage og doer, vejkode, kommunekode,...
                bla[k] = str(int(bla[k]))
    if bla['kommune'] is not None:
        bla['kommune']['kommuneNavn'] = str_clean(bla['kommune']['kommuneNavn'])
    else:
        bla['kommune'] = {'kommuneKode': None, 'kommuneNavn': None}
    return bla


def string_match(key, values, max_dist=2):
    """ Finder den naermeste string til key i values designet til adresser.
    Prøver først aa,å og å til aa som har edit distance 0 i vores verden og andre saadane kombinationer.
    Alting klares i lower case.
    """
    if key is None:
        return None
    # only tries each replacement separately
    # fancy version is edit distance where these are free...
    repl = [('å', 'aa'),
            ('kgs.', 'kongens'),
            ('kgs ', 'kongens'),
            ('sdr ', 'søndre '),
            ('sdr.', 'søndre'),
            ('sdr ', 'sønder '),
            ('sdr.', 'sønder'),
            ('sdr. ', 'sønder'),
            ('ndr ', 'nordre '),
            ('ndr.', 'nordre'),
            ('skt.', 'sankt'),
            ('skt ', 'sankt '),
            ('gl ', ' gammel '),
            ('gl.', ' gammel ')
            ]
    # ('dr ','dronning '),
    # ('dr ','doktor ')
    for (f, t) in repl:
        tmp = ' '.join(key.replace(f, t).split())
        if tmp in values:
            return tmp
        tmp = ' '.join(key.replace(t, f).split())
        if tmp in values:
            return tmp
    
    ed_dists = np.array([Le.distance(key, x) for x in values])
    idx = np.argmin(ed_dists)
    # print('best match found: {0}'.format(values[idx]))
    shortest_dist = np.min(ed_dists)
    if ed_dists[idx] <= max_dist:
        if sum(ed_dists == shortest_dist) != 1:
            # print('multiple close .... - pick one? NO')
            return None            
        return values[idx]
    # if dawa adress is substring of closest return it
    if values[idx] in key:
        # print('closest adresse is substring of adresse we are looking for - lets return it', values[idx], key)
        return values[idx]
    return None


def gb(lst):
    """ recursive groupy by function for making list of tuples to dictionaries"""
    if len(lst[0]) == 1:
        if len(lst) != 1:
            # print('bad length {0} adding list '.format(lst))
            return [x[0] for x in lst]
        return lst[0][0]

    d = {}
    for key, val in it.groupby(lst, lambda x: x[0]):
        d[key] = gb([x[1:] for x in val])
    return d


def str_clean(s):
    if s is None:
        return None
    s.replace('"', '')
    return ' '.join(s.lower().split())


class AdressTranslater(object):
    """
    Class for making adress translation.
    Caches all the database tables fetched for the mathing and the session used.
    
    """
    def __init__(self, dawa_map, adr_tree, postnr_map, post_grupper, kmap):
        """ load data dicts from database """
        self.cache_path = os.path.join(os.path.dirname(__file__), 'workcache')
        self.adr_tree = adr_tree
        self.postnr_map = postnr_map
        self.post_grupper = post_grupper
        self.kmap = kmap
        self.my_session = requests.Session()
        self.fail = (None, 'fail')
        self.dawa_map = dawa_map
        self.gl_names = None
        
    def is_greenland(self, by):
        if self.gl_names is None:
            dat = ['nanortalik', 'søndre strømfjord', 'uummannaq', 'maniitsoq', 'godthåb', 'nuussuaq', 'mestersvig',
                   'marmorilikaappilattoq', 'jakobshavn', 'christianshåb', 'thule', 'holsteinsborg', 'ammassalik',
                   'alluitsup', 'sermersooq', 'ilulissat', 'qeqertarsuaq', 'kulusuk', 'illoqqortoormiut', 'upernavik',
                   'julianehåb', 'qasigiannguit', 'angmagssalik', 'slædepatruljen', 'narsarsuaq', 'narsaq', 'qaanaaq',
                   'thule air base', 'umanak', 'scoresbysund', 'narssaq', 'qeqertarssuaq', 'sukkertoppen', 'aasiaat',
                   'kangilinnguit', 'tasiilaq', 'egedesminde', 'station', 'ikerasassuaq', 'qaarsut', 'frederikshåb',
                   'sisimiut', 'ittoqqortoormiit', 'godhavn', 'arsuk', 'constable pynt', 'qaasuitsu',
                   'kangaatsiaq', 'pituffik', 'paamiut', 'kangerlussuaq', 'pituffik/dundas', 'danmarkshavn',
                   'kangatsiak', 'nuuk', 'qaqortoq']
            self.gl_names = set(dat)
        return by in self.gl_names

    def dawa_query(self, bl_adresse):
        """ Makes simple dawa query with betegnelse string using same session"""
        adresse = beliggenhedsadresse_to_str(bl_adresse)
        if adresse is None:
            return {}
        if adresse in self.dawa_map:
            return self.dawa_map[adresse]
        
        query_url = "https://dawa.aws.dk/datavask/adresser"
        print('Do DAWA QUERY', adresse, '\n', bl_adresse)
        # import pdb
        try:
            # with # closing(requests.get('http://httpbin.org/get', stream=True)) as r:
            req = self.my_session.get(query_url, params={'betegnelse': adresse}, timeout=32)
            j = req.json()
        except Exception as e:
            print('dawa exception', e)
            self.my_session.close()
            self.my_session = requests.Session()
            try:
                req = self.my_session.get(query_url, params={'betegnelse': adresse}, timeout=32)
                j = req.json()
            except Exception as e:
                print('one more dawa exception', e)
                print('failed address fetch')
                j = []
        self.dawa_map[adresse] = j
        return j

    def kommunenavn_til_kode(self, navn):
        """ Maps kommunenavn to kommuneid

        Args:
        -----
          navn: str
        """
        if navn in self.kmap.keys():
            return self.kmap[navn]
        return None

    def get_closest_kvh_adresse_id(self, kommunekode, vejkode, husnummer):
        """
        Return arbitrary adresse with specified kommunekode, vejkode, husnummer
        Used for husnr with several actual adresses that are not matched
        """
        husnummer = self.get_hustal(husnummer)
        res = list(self.adr_tree[kommunekode][vejkode].keys())
        husnr = np.array([self.get_hustal(x) for x in res])
        husnumre_diff = np.abs(husnr - husnummer)
        near = int(np.argmin(husnumre_diff))
        eh = str(res[near])        
        rem = self.adr_tree[kommunekode][vejkode][eh]
        z = next(iter(rem.values()))
        z2 = next(iter(z.values()))
        return z2

    def get_kv_adress(self, kommunekode, vejkode):
        """
        Return arbitrary adresse med the specified kommunekode, vejkode
        Used when we do not have husnr
        """
        if kommunekode not in self.adr_tree.keys():
            return self.fail
        if vejkode not in self.adr_tree[kommunekode].keys():
            return self.fail
        
        rem = self.adr_tree[kommunekode][vejkode]
        z = next(iter(rem.values()))
        z2 = next(iter(z.values()))
        z3 = next(iter(z2.values()))
        return z3, 'kvAdresse'
    
    def get_hustal(self, st):
        """ Remove letters from husnr """
        dig = re.findall('\d+', st)
        assert len(dig) == 1, 'flere tal i husnummer {0}'.format(dig)
        return int(dig[0])    

    def brug_post2(self, postnr, postdistrikt, vejnavn, husnummer, bla):
        """ Check for small errors in postnr """    
        kg = self.post_grupper        
        
        if postdistrikt is not None and postdistrikt.split()[0] in kg.keys():
            # print('fandt by med flere postnumre')
            close = [self.postnr_map[x] for x in kg[postdistrikt.split()[0]]]
        else:
            close = []
            for k, v in kg.items():
                if postnr in v:
                    # print('fandt postnr i by med flere postnumre')
                    close = [self.postnr_map[x] for x in kg[postdistrikt]]
                    break
            if len(close) == 0:
                # print('tager bare dem der er indenfor 100 i postnumre men vejnavnet
                # skal vaere perfekt saa, stavefejl ikke tilladt her')
                lst = [(int(k), k, v) for k, v in self.postnr_map.items()]
                # pst2 = int(postnr)
                close = [x[2] for x in lst if abs(x[0] - postnr) < 100]
        
        har_vej = set()
        for p2 in close:
            for distrikt, rc in p2.items():
                # consider adding spell error here                
                if vejnavn in rc.keys():
                    # print('found in distrikt {0}'.format(distrikt))
                    res = rc[vejnavn]
                    if isinstance(res, list):
                        for kv in res:                        
                            har_vej.add(kv)
                    else:
                        har_vej.add(res)
                
        if len(har_vej) > 0:
            return self.brug_husnummer(list(har_vej), bla)
        return None, None

    def brug_husnummer(self, tmp, bla):
        """ Use husnr to find the best match in the list """
        if len(tmp) == 1:
            assert len(tmp[0]) == 2, "BAD TUPLE"
            return tmp[0]
        else:
            husnr = get_husnr(bla['husnummerFra'], bla['bogstavFra'])
            for z in tmp:
                tr_left = self.adr_tree[z[0]][z[1]]                
                if husnr in tr_left.keys():
                    return z
            # husnr_bogstav = get_husnr(bla['husnummerFra'], bla['bogstavFra'])
            husnr_tal = bla['husnummerFra']
            if husnr_tal is None:
                husnr_tal = 0
            
            etage = bla['etage']
            doer = bla['sidedoer']
            # lav tuple ordning på matchet
            poss = [(self.adr_tree[z[0]][z[1]], z) for z in tmp]

            lst = [(hr, et, do) for z in poss for (hr, r1) in z[0].items() for (et, r2) in r1.items() for do in r2]
            idl = [z[1] for z in poss for (hr, r1) in z[0].items() for (et, r2) in r1.items() for do in r2]
            ordered_list = [(abs(self.get_hustal(x[0]) - husnr_tal), x[1] != etage, x[2] != doer) for x in lst]
            idx = min(enumerate(ordered_list), key=lambda x: x[1])[0]
            # best_obj = lst[idx]
            kv = idl[idx]
            return kv

    def brug_post_og_vejnavn(self, bla):
        """
        Try and use postnr and roadname to find adress
        Added several hardcoded fixes for postnr for larger cities 
        """        
        postnr = bla['postnummer']
        if postnr is None:
            # print('ingen postnummer... ')
            return None, None
        if postnr == 8100:
            # print('fixing aarhus c postnr')
            postnr = 8000
        if postnr == 9100:
            postnr = 9000
            # print('fixing aalborg c postnr')
        if postnr == 5100:
            postnr = 5000
            # print('fixing odense c postnr')
             
        vejnavn = str_clean(bla['vejnavn'])
        postdistrikt = bla['postdistrikt']
        if postnr in self.postnr_map.keys() and len(self.postnr_map[postnr].keys()) == 1:
            # print('only one city, lets use that')
            postdistrikt = next(iter((self.postnr_map[postnr].keys())))
                        
        husnummer = bla['husnummerFra']
        # print('parsing. postnr: {0} postdistrikt: {1} vejnavn: {2}:
        # husnummer: {3}'.format(postnr, postdistrikt, vejnavn, husnummer))

        if postnr not in self.postnr_map.keys():
            # print('postnummer ikke fundet giver op - kunne proeve taet paa',postnr)
            return self.brug_post2(postnr, postdistrikt, vejnavn, husnummer, bla)
        
        if postdistrikt not in self.postnr_map[postnr].keys():
            # print('Postdistrikt match fejler {0}'.format(postdistrikt))
            _postdistrikt = string_match(postdistrikt, list(self.postnr_map[postnr].keys()))
            if _postdistrikt is None:
                # print('postdistrikt match fejlet','proev fuldt match')
                return self.brug_post2(postnr, postdistrikt, vejnavn, husnummer, bla)
            postdistrikt = _postdistrikt
            
        if vejnavn not in self.postnr_map[postnr][postdistrikt].keys():
            _vejnavn = string_match(vejnavn, list(self.postnr_map[postnr][postdistrikt].keys()))
            if _vejnavn is None:
                # print('vejnavn match fejlet','proev fuldt match')
                return self.brug_post2(postnr, postdistrikt, vejnavn, husnummer, bla)
            vejnavn = _vejnavn
      
        # print('parsed to', postnr, postdistrikt, vejnavn, self.postnr_map[postnr][postdistrikt][vejnavn])
        tmp = self.postnr_map[postnr][postdistrikt][vejnavn]    
        if isinstance(tmp, tuple):
            # print('tuple found returning it', tmp)
            return tmp
        else:
            # print('more options - lets check husnummer')
            return self.brug_husnummer(tmp, bla)

    def dawa_lookup(self, bla):
        """
        Make a loopup at dawa.aws.dk on postnr,vejnavn,husnr
        If more than one hit do heuristic based best match
        """
        
        obj = self.dawa_query(bla)
        if len(obj) == 0:
            # print('nothing returned')
            return self.fail
        result_list = obj['resultater']
        if len(result_list) == 0:
            # print('DAWA nothing returned')
            return self.fail
        best_obj = result_list[0]
        if len(result_list) > 1:
            # print('AMBIGuiTY')
            scores = [a['vaskeresultat']['forskelle'] for a in result_list]
            very_good_scores = [a for x, a in zip(scores, result_list) if x['postnr'] == 0 and x['postnrnavn'] == 0 and
                                x['vejnavn'] == 0]
            nr_vej_scores = [a for x, a in zip(scores, result_list) if x['postnr'] == 0 and x['vejnavn'] == 0]
            navn_vej_scores = [a for x, a in zip(scores, result_list) if x['postnrnavn'] == 0 and x['vejnavn'] == 0]
            # what about postdistrikt
            if len(nr_vej_scores) + len(navn_vej_scores) == 0:
                # print('BAD DAWA KV MATCH')
                return self.fail

            if len(very_good_scores) > 0:
                # print('nr, navn, vej hit')
                good_list = very_good_scores
                
            elif len(nr_vej_scores) > 0 and len(navn_vej_scores) > 0:
                # print('DISAGREEMENT i postnr postnavn problem pick both')
                good_list = navn_vej_scores + nr_vej_scores
            elif len(nr_vej_scores) > 0:
                # print('nr vej hit')
                good_list = nr_vej_scores
            else:
                # print('navn vej hit')
                good_list = navn_vej_scores
            
            # good_list = list(set(good_list))
            # get_closest_kvh_adresse_id(self, kommunekode, vejkode, husnummer)
            husnr_bogstav = get_husnr(bla['husnummerFra'], bla['bogstavFra'])
            husnr_tal = bla['husnummerFra']
            if husnr_tal is None:
                husnr_tal = 0
            
            etage = bla['etage']
            doer = bla['sidedoer']
            # lav tuple ordning på matchet      
            ordered_list = [(
                x['aktueladresse']['husnr'] != husnr_bogstav,
                abs(self.get_hustal(x['aktueladresse']['husnr']) - husnr_tal),
                x['aktueladresse']['etage'] != etage,
                x['aktueladresse']['dør'] != doer,
            ) for x in good_list]
            idx = min(enumerate(ordered_list), key=lambda x: x[1])[0]
            best_obj = good_list[idx]
                
        if 'href' not in best_obj['aktueladresse'].keys():            
            # print('Ingen href - nedlagt adresse? {0}'.format(best_obj['aktueladresse']['status']))
            # husnr = best_obj['aktueladresse']['husnr']
            # postnr = best_obj['aktueladresse']['postnr']
            # postnrnavn = best_obj['aktueladresse']['postnrnavn']
            # adresseringsvejnavn = best_obj['aktueladresse']['adresseringsvejnavn']
            # vejnavn = best_obj['aktueladresse']['vejnavn']
            # what to do with this...
            return None, 'nedlagt_adresse'
        idx = best_obj['aktueladresse']['id']
        return idx, 'addresse'

    def adresse_id(self, bla):
        """
        Find the dawa adresse id that corresponds to the beliggenhedsadresse given.
        * KOMMUNEKODE
        Options: Kommunekode, Kommunenavn, Postnummer (vejnavn), Postdistrikt        
        * Vejkode 
        Options: Vejkode, Vejnavn.
        * Husnr, Etage, Doer ###
        Options: Husnr, sideDoer, Etage
        """
        if 'adresseId' in bla and bla['adresseId'] is not None:
            return bla['adresseId'], 'adresse'
        bla = clean_adresse(bla)
        if bla['landekode'] != 'dk':
            return None, 'udland_{0}'.format(bla['landekode'])
        if self.is_greenland(bla['postdistrikt']):
            return None, 'udland_gl'
        fake_vejnavne = {'folkeregistret', 'ukendt adresse', 'uden fast bopæl', 'nordisk flytning afventes',
                         'retur til afsender', 'folkeregisteret', 'adressen ukendt', 'folkeregisteret',
                         'udsendte af den danske stat', 'ukendt adresse,retur afsender', 'retur afsender',
                         'kommunekontoret', 'uden fast bopæl', 'folkeregisteret', 'folkeregistret', 'herlev rådhus'}
        # morsø folkeregister , uk.adr.ret.t.afs
        if bla['vejnavn'] in fake_vejnavne or bla['postnummer'] == 9999:
            # print('ukendt adresse i vejnavn or postnummer 9999')
            return None, 'fake adresse'
                  
        kommune = bla['kommune']  # maybe find last here
        vejkode = None
        if kommune['kommuneKode'] is not None and int(kommune['kommuneKode']) in self.adr_tree.keys():
            kommunekode = int(kommune['kommuneKode'])
        elif kommune['kommuneKode'] is None and kommune['kommuneNavn'] is not None:
            # fejler gamlekommuner
            # print('using kommunenavn', kommune['kommuneNavn'])
            kommunekode = self.kommunenavn_til_kode(kommune['kommuneNavn'])
            if kommunekode is None:
                # print('no good kommuneinfo - try postnummer vejnavn combination')
                kommunekode, vejkode = self.brug_post_og_vejnavn(bla)
                if kommunekode is None:
                    return self.dawa_lookup(bla)
        else:
            # print('ingen god kommuneinfo - proev postnummer vejnavn kombi')
            kommunekode, vejkode = self.brug_post_og_vejnavn(bla)
            if kommunekode is None:
                return self.dawa_lookup(bla)
                            
        # VEJKODE
        if vejkode is None:
            if bla['vejkode'] is not None and bla['vejkode'] in self.adr_tree[kommunekode].keys():
                vejkode = int(bla['vejkode'])            
            # elif bla['vejnavn'] is not None:
            #     vejkode = self.vejnavn_til_kode(kommunekode, bla['vejnavn'], bla['husnummerFra'])
            #     if vejkode is None:
            #         print('vejnavn ikke fundet ordentligt proev dawa vask og google')
            #         return self.dawa_lookup(bla)
            else:
                # if bla['vejnavn'] is None and bla['postboks'] is not None:
                #    return self.post_adresse(bla)
                # print('ingen god vejkode info - proev postnummer vejnavn kombi', bla['vejkode'])
                kommunekode, vejkode = self.brug_post_og_vejnavn(bla)
                if kommunekode is None:
                    # print('postnummer  vejnavn failed')
                    return self.dawa_lookup(bla)
                # print('found kommunekode vejkode', kommunekode, vejkode)
        
        assert vejkode in self.adr_tree[kommunekode].keys()
        # Husnummer     
        if bla['husnummerFra'] is not None:
            huskode = get_husnr(bla['husnummerFra'], bla['bogstavFra'])
        elif bla['husnummerTil'] is not None:
            huskode = get_husnr(bla['husnummerTil'], bla['bogstavFra'])
        else:
            return self.get_kv_adress(kommunekode, vejkode)
                    
        if huskode not in self.adr_tree[kommunekode][vejkode].keys():            
            # print('kommuekode, vejkode, huskode {0},{1},{2} not found try combine
            # huskode med doer'.format(kommunekode, vejkode, huskode))
            return self.get_closest_kvh_adresse_id(kommunekode, vejkode, huskode), 'kvAdresse'
        # print('kommuekode, vejkode, huskode {0}, {1}, {2} found'.format(kommunekode, vejkode, huskode))
        # parse etage og doer med
        etage = bla['etage']
        doer = bla['sidedoer']
        
        tree_left = self.adr_tree[kommunekode][vejkode][huskode]
        if etage in tree_left.keys():
            if doer in tree_left[etage].keys():
                return tree_left[etage][doer], 'adresse'
            else:
                return next(iter(tree_left[etage].values())), 'adresse'
        else:
            # print('etage doer not found')
            aid = next(iter((next(iter(tree_left.values()))).values()))
            if len(tree_left) == 1:
                "if only one adresse left, take it"
                return aid, 'adresse'
            else:
                return aid, 'kvhAdresse'
