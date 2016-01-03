# -*- coding: utf-8 -*-
from dsl.took import Step


class UnivBasicTask(object):
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


class UnivFetchTask(object):
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
        mysql_syn_step['actiontype'] = 'fetch'
        yield mysql_syn_step


class UnivNameTask(UnivFetchTask):

    tkey = 'UnivNameTask'

    def __init__(self):
        self.mysql_template = "SELECT cn.NAME FROM college_name as cn\
            WHERE cn.UNITID = %(UNITID)s and (cn.LANG = 'EN' or cn.LANG = 'CN')"


class UnivSlugTask(UnivBasicTask):

    tkey = 'UnivSlugTask'

    def __init__(self):
        self.mysql_template = "SELECT cn.UNITID FROM college_name as cn WHERE cn.SLUG = %(SLUG)s"


class UnivLocateTask(UnivBasicTask):
    '''
    tab: 位置
    subtab: 地图
    '''
    tkey = 'UnivLocateTask'

    def __init__(self):
        self.mysql_template = "SELECT ff.LATITUDE, ff.LONGITUD, ff.STABBR, ff.CITY, ff.ADDR, ff.GENTELE \
        FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s;"


class UnivEnrollmentTask(UnivBasicTask):
    '''
    tab: 学生情况
    subtab: 招生明细
    '''
    tkey = 'UnivEnrollmentTask'

    def __init__(self):
        self.mysql_template = "SELECT ai.EFTOTLT_TOTAL, ai.EFTOTLT_GR, ai.EFTOTLT_UNGR, ai.ENRLT \
        FROM college_ai as ai WHERE ai.UNITID = %(UNITID)s;"


class UnivEthnicityTask(UnivBasicTask):
    '''
    tab: 学生情况
    subtab: 学生统计
    selecttab: 人种
    '''
    tkey = 'UnivEthnicityTask'

    def __init__(self):
        self.mysql_template = "SELECT ai.EFTOTLT_TOTAL, ai.EFWHITT, ai.EFBKAAT, ai.EFASIAT \
        FROM college_ai as ai WHERE ai.UNITID = %(UNITID)s;"


class UnivEthnicityStateTask(UnivFetchTask):
    '''
    tab: 学生情况
    subtab: 学生统计
    selecttab: 人种 同州平均
    '''
    tkey = 'UnivEthnicityStateTask'

    def __init__(self):
        self.mysql_template = "SELECT sdet.*, sdic.VALUEEN \
            FROM stati_details as sdet, stati_category as scat, stati_dict as sdic where sdet.CATEGORY_ID in (\
            SELECT scat.id FROM stati_category as scat WHERE scat.LEVEL = 3 and scat.REGON = (\
            SELECT STABBR FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s) and\
            scat.FTYPEID = 28) and sdet.CATEGORY_ID = scat.id and scat.TYPEID = sdic.id;"


class UnivGenderTask(UnivBasicTask):
    '''
    tab: 学生情况
    subtab: 学生统计
    selecttab: 性别
    '''
    tkey = 'UnivGenderTask'

    def __init__(self):
        self.mysql_template = "SELECT ai.EFTOTLT_TOTAL, ai.EFTOTLM, ai.EFTOTLW \
        FROM college_ai as ai WHERE ai.UNITID = %(UNITID)s;"


class UnivGenderStateTask(UnivFetchTask):
    '''
    tab: 学生情况
    subtab: 学生统计
    selecttab: 性别 同州平均
    '''
    tkey = 'UnivGenderStateTask'

    def __init__(self):
        self.mysql_template = "SELECT sdet.*, sdic.VALUEEN \
            FROM stati_details AS sdet, stati_category as scat, stati_dict as sdic where sdet.CATEGORY_ID in (\
            SELECT scat.id FROM stati_category as scat WHERE scat.LEVEL = 3 and scat.REGON = (\
            SELECT STABBR FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s) and\
            scat.FTYPEID = 26) and sdet.CATEGORY_ID = scat.id and scat.TYPEID = sdic.id;"


class UnivMajorNumTask(UnivBasicTask):
    '''
    tab: 专业
    subtab: 专业情况
    selecttab: 专业数
    '''
    tkey = 'UnivMajorNumTask'

    def __init__(self):
        self.mysql_template = "SELECT COUNT(c_major.MAJOR) as MAJORNUM FROM (\
            SELECT ccomp.CIPCODE as MAJOR FROM college_comp as ccomp\
            WHERE ccomp.UNITID = %(UNITID)s and ccomp.CIPCODE != 99\
            GROUP BY ccomp.CIPCODE) as c_major;"


class UnivMajorTask(UnivFetchTask):
    '''
    tab: 专业
    subtab: 专业情况
    selecttab: 专业详细信息
    '''
    tkey = 'UnivMajorTask'

    def __init__(self):
        self.mysql_template = "SELECT CIPCODE, sum(CTOTALT) as CTOTALT\
            FROM college_comp as ccomp\
            WHERE ccomp.UNITID = %(UNITID)s and ccomp.CIPCODE != 99\
            GROUP BY ccomp.CIPCODE ORDER BY CTOTALT;"


class UnivTuitionOnCampusTask(UnivBasicTask):
    '''
    tab: 学费
    subtab: 基本费用
    selecttab: 住校基本费用
    '''
    tkey = 'UnivTuitionOnCampusTask'

    def __init__(self):
        self.mysql_template = "SELECT tui.CHG3AY3, tui.CHG4AY3, tui.CHG5AY3, tui.CHG6AY3 \
        FROM college_tuition as tui WHERE tui.UNITID=%(UNITID)s;"


class UnivTuitionOffCampusTask(UnivBasicTask):
    '''
    tab: 学费
    subtab: 基本费用
    selecttab: 住校基本费用
    '''
    tkey = 'UnivTuitionOffCampusTask'

    def __init__(self):
        self.mysql_template = "SELECT tui.CHG3AY3, tui.CHG4AY3, tui.CHG7AY3, tui.CHG8AY3\
        FROM college_tuition as tui WHERE tui.UNITID=%(UNITID)s;"


class UnivTuitionCompareTask(UnivFetchTask):
    '''
    tab: 学费
    subtab: 基本费用
    selecttab: 学费对比
    '''
    tkey = 'UnivTuitionCompareTask'

    def __init__(self):
        self.mysql_template = "SELECT sdet.*, scat.REGON,viewd.VALUE_EN from stati_details as sdet, stati_category as scat,  view_dict_us_regon as viewd\
            WHERE( sdet.CATEGORY_ID in (\
            SELECT scat.id FROM stati_category as scat WHERE \
            (scat.REGON = (SELECT OBEREG FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s) or \
            scat.REGON = (SELECT STABBR FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s))\
            and scat.TYPEID = 12)\
            or sdet.CATEGORY_ID = (SELECT scat.id FROM stati_category as scat WHERE scat.REGON = 'US' and scat.TYPEID = 12))\
            and scat.id = sdet.CATEGORY_ID \
            and scat.REGON = viewd.ID;"


class UnivAdmiBasicTask(UnivBasicTask):
    '''
    tab: 录取
    subtab: 录取情况
    '''
    tkey = 'UnivAdmiBasicTask'

    def __init__(self):
        self.mysql_template = "SELECT cai.APPLCN,cai.ADMSSN, cai.ADMSSN_PERC, cai.ENRLT, cai.ENRLT_PERC\
            FROM college_ai as cai WHERE cai.UNITID=%(UNITID)s;"


class UnivAdmiReqTask(UnivBasicTask):
    '''
    tab: 录取
    subtab: 录取标准
    '''
    tkey = 'UnivAdmiReqTask'

    def __init__(self):
        self.mysql_template = "SELECT cai.ADMCON1,cai.ADMCON2,cai.ADMCON3,cai.ADMCON5,cai.ADMCON7,cai.ADMCON8\
        FROM college_ai as cai WHERE cai.UNITID=%(UNITID)s;"


class UnivAdmiUrlTask(UnivBasicTask):
    '''
    tab: 录取
    subtab: 申请信息
    '''
    tkey = 'UnivAdmiUrlTask'
    def __init__(self):
        self.mysql_template = "SELECT ff.WEBADDR, ff.ADMINURL, ff.APPLURL, ff.FAIDURL, ff.NPRICURL\
            FROM college_ff as ff WHERE ff.UNITID=%(UNITID)s;"


class UnivAdmiSticTask(UnivFetchTask):
    '''
    tab: 录取
    subtab: 录取情况 - 统计信息
    '''
    tkey = 'UnivAdmiSticTask'
    def __init__(self):
        self.mysql_template = "SELECT sdet.*, sdic.VALUEEN from stati_details as sdet, stati_category as scat, stati_dict as sdic\
            where sdet.CATEGORY_ID in (\
            SELECT scat.id FROM stati_category as scat WHERE scat.LEVEL = 3 and scat.REGON = (\
            SELECT STABBR FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s) and\
            scat.FTYPEID = 30) and sdet.CATEGORY_ID = scat.id and scat.TYPEID = sdic.id;"
