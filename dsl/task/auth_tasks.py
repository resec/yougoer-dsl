from dsl.took import Step

class AuthTask(object):

    def steps(self, param, result):
        auth_type = param['auth_type']

        if auth_type == 'sign_in':
            mysql_syn_step = Step('MysqlHandler')
        elif auth_type == 'login':
            pass
        elif auth_type == 'reset_password':
            pass
        else:
            pass
            