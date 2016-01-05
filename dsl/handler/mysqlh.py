
from mysql.connector import connect, Error, errorcode
from mysql.connector.errors import ProgrammingError

class MysqlHandler(object):


    hkey = 'MysqlHandler'

    dbconfig = {
        "database":"YOUGOER",
        "user":"root",
        "password":"chenzhongming",
        "host":"localhost",
        'charset':'utf8mb4',
    }

    action_method_map = {
        'execute':'_execute',
        'fetch':'_fetch',
        'fetch_one':'_fetch_one',
        'callproc':'_callproc',
    }


    def action(self, step, param):
        template = step['template']
        actiontype = None if 'actiontype' not in step else step['actiontype']

        if not actiontype or actiontype not in self.action_method_map:
            raise ValueError('Invalid actiontype %s' % actiontype)
        else:
            method = getattr(self, self.action_method_map[actiontype])

        #print(method)
        return method(template, param)


    def _get_connection(self):
        try:
            return connect(pool_size = 20, **MysqlHandler.dbconfig)
        except Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)


    def _execute(self, sql, param=None):
        cnx = self._get_connection()
        cursor = cnx.cursor(buffered=True, dictionary=True)

        try:
            cursor.execute(operation=sql, params=param)
            rowcount = cursor.rowcount
            if rowcount is None:
                rowcount = 0
            result = {'row_impacted':rowcount}
        except Exception as pe:
            result = dict(error='error in executing sql %s with param %s, error: %s' % (sql, param, str(pe)))

        cursor.close()
        cnx.commit()
        cnx.close()

        return result


    def _callproc(self, sql, param=None):
        raise ValueError("callproc is not yet implemented")


    def _fetch(self, sql, param=None):
        cnx = self._get_connection()
        cursor = cnx.cursor(buffered=True)

        try:
            cursor.execute(operation=sql, params=param)
            result = {'columns':cursor.column_names, 'rows':cursor.fetchall()}
        except Exception as pe:
            result = dict(error='error in executing sql %s with param %s, error: %s' % (sql, param, str(pe)))

        cursor.close()
        cnx.commit()
        cnx.close()

        if result is None:
            result = {'columns':[], 'rows':{}}

        return result


    def _fetch_one(self, sql, param=None):
        cnx = self._get_connection()
        cursor = cnx.cursor(buffered=True, dictionary=True)

        try:
            cursor.execute(operation=sql, params=param)
            result = cursor.fetchone()
        except Exception as pe:
            result = dict(error='error in executing sql %s with param %s, error: %s' % (sql, param, str(pe)))

        cursor.close()
        cnx.commit()
        cnx.close()

        if result is None:
            result = {}

        return result
