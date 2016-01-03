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


class CollegeFetchTask(object):
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


class CollegeNameTask(CollegeBasicTask):

    tkey = 'CollegeNameTask'

    def __init__(self):
        self.mysql_template = "SELECT cn.NAME FROM college_name as cn WHERE cn.UNITID = %(UNITID)s and cn.LANG = 'EN'"


class CollegeSlugTask(CollegeBasicTask):

    tkey = 'CollegeSlugTask'

    def __init__(self):
        self.mysql_template = "SELECT cn.UNITID FROM college_name as cn WHERE cn.SLUG = %(SLUG)s"


class ColUSLocateTask(CollegeBasicTask):
    '''
    tab: 位置
    subtab: 地图
    '''
    tkey = 'ColUSLocateTask'

    def __init__(self):
        self.mysql_template = "SELECT ff.LATITUDE, ff.LONGITUD, ff.STABBR, ff.CITY, ff.ADDR, ff.GENTELE \
        FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s;"


class ColUSEnrollmentTask(CollegeBasicTask):
    '''
    tab: 学生情况
    subtab: 招生明细
    '''
    tkey = 'ColUSEnrollmentTask'

    def __init__(self):
        self.mysql_template = "SELECT ai.EFTOTLT_TOTAL, ai.EFTOTLT_GR, ai.EFTOTLT_UNGR, ai.ENRLT \
        FROM college_ai as ai WHERE ai.UNITID = %(UNITID)s;"


class ColUSEthnicityTask(CollegeBasicTask):
    '''
    tab: 学生情况
    subtab: 学生统计
    selecttab: 人种
    '''
    tkey = 'ColUSEthnicityTask'

    def __init__(self):
        self.mysql_template = "SELECT ai.EFTOTLT_TOTAL, ai.EFWHITT, ai.EFBKAAT, ai.EFASIAT \
        FROM college_ai as ai WHERE ai.UNITID = %(UNITID)s;"


class ColUSEthnicityStateTask(CollegeFetchTask):
    '''
    tab: 学生情况
    subtab: 学生统计
    selecttab: 人种 同州平均
    '''
    tkey = 'ColUSEthnicityStateTask'

    def __init__(self):
        self.mysql_template = "SELECT sdet.*, sdic.VALUEEN \
            FROM stati_details as sdet, stati_category as scat, stati_dict as sdic where sdet.CATEGORY_ID in (\
            SELECT scat.id FROM stati_category as scat WHERE scat.LEVEL = 3 and scat.REGON = (\
            SELECT STABBR FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s) and\
            scat.FTYPEID = 28) and sdet.CATEGORY_ID = scat.id and scat.TYPEID = sdic.id;"


class ColUSGenderTask(CollegeBasicTask):
    '''
    tab: 学生情况
    subtab: 学生统计
    selecttab: 性别
    '''
    tkey = 'ColUSGenderTask'

    def __init__(self):
        self.mysql_template = "SELECT ai.EFTOTLT_TOTAL, ai.EFTOTLM, ai.EFTOTLW \
        FROM college_ai as ai WHERE ai.UNITID = %(UNITID)s;"


class ColUSGenderStateTask(CollegeFetchTask):
    '''
    tab: 学生情况
    subtab: 学生统计
    selecttab: 性别 同州平均
    '''
    tkey = 'ColUSGenderStateTask'

    def __init__(self):
        self.mysql_template = "SELECT sdet.*, sdic.VALUEEN \
            FROM stati_details AS sdet, stati_category as scat, stati_dict as sdic where sdet.CATEGORY_ID in (\
            SELECT scat.id FROM stati_category as scat WHERE scat.LEVEL = 3 and scat.REGON = (\
            SELECT STABBR FROM college_ff as ff WHERE ff.UNITID = %(UNITID)s) and\
            scat.FTYPEID = 26) and sdet.CATEGORY_ID = scat.id and scat.TYPEID = sdic.id;"


class ColUSMajorNumTask(CollegeBasicTask):
    '''
    tab: 专业
    subtab: 专业情况
    selecttab: 专业数
    '''
    tkey = 'ColUSMajorNumTask'

    def __init__(self):
        self.mysql_template = "SELECT COUNT(c_major.MAJOR) as MAJORNUM FROM (\
            SELECT ccomp.CIPCODE as MAJOR FROM YOUGOER.college_comp as ccomp\
            WHERE ccomp.UNITID = %(UNITID)s and ccomp.CIPCODE != 99\
            GROUP BY ccomp.CIPCODE) as c_major;"


class ColUSMajorTask(CollegeFetchTask):
    '''
    tab: 专业
    subtab: 专业情况
    selecttab: 专业详细信息
    '''
    tkey = 'ColUSMajorTask'

    def __init__(self):
        self.mysql_template = "SELECT CIPCODE, sum(CTOTALT) as CTOTALT\
            FROM YOUGOER.college_comp as ccomp\
            WHERE ccomp.UNITID = %(UNITID)s and ccomp.CIPCODE != 99\
            GROUP BY ccomp.CIPCODE ORDER BY CTOTALT;"


class ColUSTuitionOnCampusTask(CollegeBasicTask):
    '''
    tab: 学费
    subtab: 基本费用
    selecttab: 住校基本费用
    '''
    tkey = 'ColUSTuitionOnCampusTask'

    def __init__(self):
        self.mysql_template = "SELECT tui.CHG3AY3, tui.CHG4AY3, tui.CHG5AY3, tui.CHG6AY3 \
        FROM YOUGOER.college_tuition as tui WHERE tui.UNITID=%(UNITID)s;"


class ColUSTuitionOffCampusTask(CollegeBasicTask):
    '''
    tab: 学费
    subtab: 基本费用
    selecttab: 住校基本费用
    '''
    tkey = 'ColUSTuitionOffCampusTask'

    def __init__(self):
        self.mysql_template = "SELECT tui.CHG3AY3, tui.CHG4AY3, tui.CHG7AY3, tui.CHG8AY3\
        FROM YOUGOER.college_tuition as tui WHERE tui.UNITID=%(UNITID)s;"



