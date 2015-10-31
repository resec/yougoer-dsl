# -*- coding: utf-8 -*-
from dsl.took import Step


class CollegeBasicTask(object):
    '''
    Basic College Task
    '''

    def __init__(self):
        self.mysql_template = ""

    def steps(self, param, result):
        '''
        param:UNITID
        '''
        mysql_syn_step = Step('MysqlHandler')
        mysql_syn_step['template'] = self.mysql_template
        mysql_syn_step['actiontype'] = 'fetch_one'
        yield mysql_syn_step


class CollegeFastFactsTask(CollegeBasicTask):
    '''
    Fast Facts/ Quick Stats
    -- select the basic information of the college
    '''
    tkey = 'CollegeFastFactsTask'

    def __init__(self):
        self.mysql_template = "SELECT \
            ff.ADDR, ff.CITY, ff.LATITUDE, ff.LONGITUD, ff.WEBADDR, ff.ADMINURL,\
            co.ABBREVIATION as COUNTRY,\
            cusstabbr.VALUE_EN as STABBR,\
            cusobereg.VALUE_EN as OBEREG,\
            cusccbasic.VALUE_EN as CCBASIC,\
            cussector.VALUE_EN as SECTOR,\
            cuscalsys.VALUE_EN as CALSYS,\
            cusinstcat.VALUE_EN as INSTCAT,\
            cuslocale.VALUE_EN as LOCALE,\
            cusinstsize.VALUE_EN as INSTSIZE,\
            cushloffer.VALUE_EN as HLOFFER,\
            cusadmcon1.VALUE_EN as UGOFFER,\
            cusadmcon2.VALUE_EN as GROFFER\
            FROM\
            college_ff as ff,\
            college_country as co,\
            college_us_stabbr as cusstabbr,\
            college_us_obereg as cusobereg,\
            college_us_ccbasic as cusccbasic,\
            college_us_sector as cussector,\
            college_us_calsys as cuscalsys,\
            college_us_instcat as cusinstcat,\
            college_us_locale as cuslocale,\
            college_us_instsize as cusinstsize,\
            college_us_hloffer as cushloffer,\
            college_us_admcon as cusadmcon1,\
            college_us_admcon as cusadmcon2\
            WHERE\
            ff.UNITID = %(UNITID)s and\
            ff.COUNTRY_ID = co.id and\
            cusstabbr.ID = ff.STABBR and\
            cusobereg.ID = ff.OBEREG and\
            cusccbasic.ID = ff.CCBASIC and\
            cussector.ID = ff.SECTOR and\
            cuscalsys.ID = ff.CALSYS and\
            cusinstcat.ID = ff.INSTCAT and\
            cuslocale.ID = ff.LOCALE and\
            cusinstsize.ID = ff.INSTSIZE and\
            cushloffer.ID = ff.HLOFFER and\
            cusadmcon1.ID = ff.UGOFFER and\
            cusadmcon2.ID = ff.GROFFER"


class CollegeAdmiInfoTask(CollegeBasicTask):
    '''
    Admisssion information
    -- select 5, Applicants total, acceptance rate, acceptance, percent yield , total enrolled(not in order)
    '''
    tkey = 'CollegeAdmiInfoTask'

    def __init__(self):
        self.mysql_template = "SELECT ai.APPLCN, ai.ADMSSN, ai.ADMSSN_PERC, ai.ENRLT, ai.ENRLT_PERC \
            FROM college_ai as ai WHERE ai.UNITID = %(UNITID)s"


class CollegeStuInfoKFTask(CollegeBasicTask):
    '''
    Student Breakdown/ Student information
    for basic info
    @@ select 4, total undergradute, total gradute, total enrolled, student-faculty ratio
    '''
    tkey = 'CollegeStuInfoKFTask'

    def __init__(self):
        self.mysql_template = "SELECT ai.EFTOTLT_TOTAL, ai.EFTOTLT_GR, ai.EFTOTLT_UNGR, ai.STUFACR, \
            ai.ENROLLUPRE, ai.ENROLLGPRE \
            FROM college_ai as ai WHERE ai.UNITID = %(UNITID)s"


class CollegeStuInfoETHTask(CollegeBasicTask):
    '''
    Student Breakdown/ Student information
    @ for ethric group
    @@ select 5, total man, total woman, total white, total back, total asia students
    '''
    tkey = 'CollegeStuInfoETHTask'

    def __init__(self):
        self.mysql_template = "SELECT ai.EFTOTLM, ai.EFTOTLW, ai.EFWHITT, ai.EFBKAAT, ai.EFASIAT  \
            FROM college_ai as ai WHERE ai.UNITID = %(UNITID)s"


class CollegeTuiFeeUnTask(CollegeBasicTask):
    '''
    Tuition & Fee
    '''
    tkey = 'CollegeTuiFeeUnTask'

    def __init__(self):
        self.mysql_template = "SELECT tu.APPLFEEU, tu.APPLFEEG, tu.CHG3AY3, tu.CHG4AY3, tu.CHG5AY3, tu.CHG6AY3, \
        tu.TUIPRE, tu.BOOPRE, tu.ROOPRE, tu.OTHPRE, tu.APPGPRE, tu.APPUPRE, tu.APPALLCUR, tu.TUIALLCUR \
            FROM college_tuition as tu WHERE tu.UNITID = %(UNITID)s"


class CollegeRankTask(CollegeBasicTask):
    '''
    Rankings & Ratings - for USNEWS Global Ranking
    @ Param : TYPE, FIELD_TYPE
    @ eg. TYPE = USNEWS, FIELD_TYPE = ALL
    '''
    tkey = 'CollegeRankTask'

    def __init__(self):
        self.mysql_template = "SELECT cr.RANK, cr.YEAR FROM college_ranking as cr \
            WHERE cr.UNITID = %(UNITID)s and cr.CATEGORY_ID in (\
            SELECT rc.id FROM ranking_category as rc \
            WHERE rc.TYPE = %(TYPE)s and rc.FIELD_TYPE = %(FIELD_TYPE)s) ORDER BY cr.YEAR ASC"

    def steps(self, param, result):
        '''
        param:UNITID
        '''
        mysql_syn_step = Step('MysqlHandler')
        mysql_syn_step['template'] = self.mysql_template
        mysql_syn_step['actiontype'] = 'fetch'
        yield mysql_syn_step


class CollegeNameTask(CollegeBasicTask):

    tkey = 'CollegeNameTask'

    def __init__(self):
        self.mysql_template = "SELECT cn.NAME FROM college_name as cn WHERE cn.UNITID = %(UNITID)s and cn.LANG = 'EN'"


class CollegeSlugTask(CollegeBasicTask):

    tkey = 'CollegeSlugTask'

    def __init__(self):
        self.mysql_template = "SELECT cn.UNITID FROM college_name as cn WHERE cn.SLUG = %(SLUG)s"


class CollegeCompareTask(CollegeBasicTask):

    tkey = 'CollegeCompareTask'

    def __init__(self):
        self.mysql_template = "SELECT ff.LATITUDE, ff.LONGITUD, ff.WEBADDR, ai.ADMSSN_PERC\
            FROM college_ff as ff, college_ai as ai\
            WHERE ai.UNITID = ff.UNITID and ff.UNITID = %(UNITID)s;"
