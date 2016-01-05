# -*- coding: utf-8 -*-
from dsl.took import Step

#################################################################################
# Basic Class
#################################################################################
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


#################################################################################
# API
#################################################################################
class StatiCategory(UnivFetchTask):
    '''
    #API1 获取学生情况类别
    stdin: ('StatiCategory',{'TYPEID3':37})
    '''
    tkey = 'StatiCategory'

    def __init__(self):
        self.mysql_template = "SELECT dict.id\
            from stati_dict as dict where dict.id in\
            (select cat.TYPEID2 from stati_category as cat where cat.TYPEID3 = %(TYPEID3)s);"




class StatiDictValue(object):
    '''
    #API3 根据ID获取DICT内容
    stdin: ('StatiDictValue',{'IDS':[26, 28, 21, 22, 23, 24, 25]})
    '''
    tkey = 'StatiDictValue'

    def steps(self, param, result):
        template = "SELECT dict.id as 'KEY', dict.VALUECN as 'VALUE' from stati_dict as dict\
            where dict.id in ("

        ids = param['IDS']
        for id in ids:
            template += str(id) + ','
        template = template[:-1] + ");"
        print(template)
        mysql_syn_step = Step('MysqlHandler')
        mysql_syn_step['template'] = template
        mysql_syn_step['actiontype'] = 'fetch'
        yield mysql_syn_step


#################################################################################
# !!! HEADER
#################################################################################
class UnivNameTask(UnivFetchTask):

    tkey = 'UnivNameTask'

    def __init__(self):
        self.mysql_template = "SELECT cn.NAME FROM college_name as cn\
            WHERE cn.UNITID = %(UNITID)s and (cn.LANG = 'EN' or cn.LANG = 'CN')"


class UnivSlugTask(UnivBasicTask):

    tkey = 'UnivSlugTask'

    def __init__(self):
        self.mysql_template = "SELECT cn.UNITID FROM college_name as cn WHERE cn.SLUG = %(SLUG)s"


#################################################################################
# 地理位置
#################################################################################
class UnivLocateTask(UnivBasicTask):
    '''
    tab: 位置
    subtab: 地图
    stdin:('UnivLocateTask',{'UNITID':166027})
    '''
    tkey = 'UnivLocateTask'

    def __init__(self):
        self.mysql_template = "SELECT ff.LATITUDE, ff.LONGITUD, ff.STABBR, ff.CITY, ff.ADDR, ff.GENTELE \
        FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s;"

#################################################################################
# 学生情况
#################################################################################
class UnivEnrolAdmisTask(UnivBasicTask):
    '''
    tab: 学生情况
    subtab: 招生/录取情况
    '''
    tkey = 'UnivEnrolAdmisTask'

    def __init__(self):
        self.mysql_template = "SELECT ai.APPLCN,ai.ADMSSN, ai.ADMSSN_PERC, ai.ENRLT, ai.ENRLT_PERC, \
            ai.EFTOTLT_TOTAL, ai.EFTOTLT_GR, ai.EFTOTLT_UNGR \
            FROM college_ai as ai WHERE ai.UNITID = %(UNITID)s;"


class UnivEnrolAdmisStatiTask(UnivFetchTask):
    '''
    tab: 学生情况
    subtab: 招生情况
    selecttab: 招生统计
    stdin: ('UnivEnrolAdmisStatiTask',{'UNITID':166027})
    '''
    tkey = 'UnivEnrolAdmisStatiTask'

    def __init__(self):
        self.mysql_template = "SELECT * FROM stati_details as sdet WHERE\
            sdet.CATEGORY_ID in (SELECT id FROM stati_category WHERE TYPEID2 = 39 and TYPEID1 = 20) and\
            (sdet.REGON = 'US'\
            or sdet.REGON = %(UNITID)s\
            or sdet.REGON = (SELECT ff.OBEREG FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s)\
            or sdet.REGON = (SELECT ff.STABBR FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s))"


class UnivEthnicityStateTask(UnivFetchTask):
    '''
    tab: 学生情况
    subtab: 学生统计
    selecttab: 人种统计
    stdin: ('UnivEthnicityStateTask',{'UNITID':166027, 'TYPEID2': 28})
    '''
    tkey = 'UnivEthnicityStateTask'

    def __init__(self):
        self.mysql_template = "SELECT * FROM stati_details as sdet WHERE\
            sdet.CATEGORY_ID in (SELECT id FROM stati_category WHERE TYPEID2 = %(TYPEID2)s) and\
            (sdet.REGON = %(UNITID)s or sdet.REGON = (SELECT ff.STABBR FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s))"


class UnivGenderStateTask(UnivFetchTask):
    '''
    tab: 学生情况
    subtab: 学生统计
    selecttab: 性别-州统计
    stdin: 'UnivGenderStateTask',{'UNITID':166027, 'TYPEID2': 26}
    '''
    tkey = 'UnivGenderStateTask'

    def __init__(self):
        self.mysql_template = "SELECT * FROM stati_details as sdet WHERE\
            sdet.CATEGORY_ID in (SELECT id FROM stati_category WHERE TYPEID2 = %(TYPEID2)s) and\
            (sdet.REGON = %(UNITID)s or sdet.REGON = (SELECT ff.STABBR FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s))"


#################################################################################
# 专业情况
#################################################################################
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


#################################################################################
# 费用
#################################################################################
class UnivTuitionOnCampusTask(UnivBasicTask):
    '''
    tab: 学费
    subtab: 基本费用
    selecttab: 校内基本费用
    '''
    tkey = 'UnivTuitionOnCampusTask'

    def __init__(self):
        self.mysql_template = "SELECT tui.CHG3AY3, tui.CHG4AY3, tui.CHG5AY3, tui.CHG6AY3 \
        FROM college_tuition as tui WHERE tui.UNITID=%(UNITID)s;"


class UnivTuitionOffCampusTask(UnivBasicTask):
    '''
    tab: 学费
    subtab: 基本费用
    selecttab: 校外基本费用
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
    stdin:('UnivTuitionCompareTask',{'UNITID':166027})
    '''
    tkey = 'UnivTuitionCompareTask'

    def __init__(self):
        self.mysql_template = "SELECT * FROM stati_details as sdet WHERE\
        sdet.CATEGORY_ID in (SELECT id FROM stati_category WHERE TYPEID2 = 31) and\
        (sdet.REGON = 'US'\
        or sdet.REGON = %(UNITID)s\
        or sdet.REGON = (SELECT ff.OBEREG FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s)\
        or sdet.REGON = (SELECT ff.STABBR FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s))"


#################################################################################
# 录取情况
#################################################################################
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
        self.mysql_template = ""


#################################################################################
# 排名
#################################################################################
class UnivRankTypeTask(UnivFetchTask):
    '''
    tab: 排名
    subtab: 排名列表
    stdin:'UnivRankTypeTask',{'UNITID':166027}
    '''
    tkey = 'UnivRankTypeTask'

    def __init__(self):
        self.mysql_template = "SELECT distinct rcat.RANKTYPE\
            FROM rank_details as rdet, rank_category as rcat\
            WHERE rdet.UNITID = %(UNITID)s and rdet.CATEGORY_ID in(\
            SELECT id FROM rank_category WHERE FIELDTYPE = 'ALL') and rcat.id = rdet.CATEGORY_ID;"


class UnivRankAllTask(UnivFetchTask):
    '''
    tab: 排名
    subtab: 综合排名列表
    stdin: 'UnivRankAllTask',{'UNITID':166027, 'RANKTYPE':'USNEWS'}
    '''
    tkey = 'UnivRankAllTask'

    def __init__(self):
        self.mysql_template = "SELECT rdet.RANK, rdet.YEAR FROM rank_details as rdet WHERE\
            rdet.CATEGORY_ID in (SELECT id from rank_category WHERE RANKTYPE = %(RANKTYPE)s and FIELDTYPE = 'ALL')\
            and rdet.UNITID = %(UNITID)s order by rdet.YEAR;"


class UnivSubRankTask(UnivFetchTask):
    '''
    tab: 排名
    subtab: 热门专业排名列表
    stdin: 'UnivSubRankTask',{'UNITID':166027, 'RANKTYPE':'USNEWS', 'YEAR':2016}

    '''
    tkey = 'UnivSubRankTask'

    def __init__(self):
        self.mysql_template = "SELECT rdet.RANK, rcat.FIELDTYPE FROM rank_details as rdet, rank_category as rcat\
            WHERE rdet.CATEGORY_ID in (SELECT id from rank_category \
                WHERE RANKTYPE = %(RANKTYPE)s and FIELDTYPE != 'ALL' and USED = 1 and YEAR=%(YEAR)s) \
            and rdet.UNITID = %(UNITID)s and rcat.id = rdet.CATEGORY_ID and rdet.YEAR=%(YEAR)s order by rdet.RANK;"



#################################################################################
# 基本信息
#################################################################################
