# -*- coding: utf-8 -*-
from dsl.took import Step

class RequestTestTask(object):

    tkey = 'RequestTestTask'

    def steps(self, param, result):
        mysql_syn_step = Step('MysqlHandler')
        mysql_asyn_step = Step('MysqlHandler')
        mysql_asyn_step['asyn'] = True

        mysql_syn_step['template'] = 'select 1, 2, 3, 4, 5, $time, $country from abc'
        mysql_syn_step['actiontype'] = 'fetch'
        mysql_asyn_step['template'] = 'select 1, 2, 3, 4, 5, 6'
        mysql_asyn_step['actiontype'] = 'execute'

        yield [mysql_syn_step,
                mysql_asyn_step,
                mysql_syn_step
                ]

        mongo_syn_step = Step('MongoHandler')
        mongo_asyn_step = Step('MongoHandler')
        mongo_asyn_step['asyn'] = True
        mongo_syn_step['template'] = {'find':'restaurants','limit':2}
        mongo_syn_step['actiontype'] = 'find'
        mongo_syn_step['database'] = 'test'
        mongo_asyn_step['template'] = {'find_and_modify':"restaurants",'query':{},'sort':{},'update':{'$set':{'category': "apparel"}}}
        mongo_asyn_step['actiontype'] = 'find_and_modify'
        mongo_asyn_step['database'] = 'test'

        yield mongo_syn_step
        yield mongo_asyn_step

