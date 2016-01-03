
from mysql.connector import connect, Error, errorcode
from mysql.connector.errors import ProgrammingError

class MysqlHandler(object):


    hkey = 'MysqlHandler'

    dbconfig = {
        "database":"YOUGOER",
        "user":"root",
        "password":"yougoer",
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
            return connect(pool_size = 3, **MysqlHandler.dbconfig)
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
        except ProgrammingError as pe:
            cursor.close()
            cnx.close()
            print(str(pe))
            return dict(error='error in executing sql %s with param %s' % (sql, param))

        rowcount = cursor.rowcount
        cursor.close()
        cnx.commit()
        cnx.close()

        return {'rowcount':rowcount}


    def _callproc(self, sql, param=None):
        raise ValueError("callproc is not yet implemented")


    def _fetch(self, sql, param=None):
        cnx = self._get_connection()
        cursor = cnx.cursor(buffered=True, dictionary=True)

        try:
            cursor.execute(operation=sql, params=param)
        except ProgrammingError as pe:
            cursor.close()
            cnx.close()
            print(str(pe))
            return dict(error='error in executing sql %s with param %s' % (sql, param))

        temp = cursor.fetchall()
        rowcount = cursor.rowcount
        cursor.close()
        cnx.commit()
        cnx.close()

        if temp is None:
            temp = {}
            rowcount = 0

        return {'rows':temp, 'rowcount':rowcount}


    def _fetch_one(self, sql, param=None):
        cnx = self._get_connection()
        cursor = cnx.cursor(buffered=True, dictionary=True)
        
        try:
            cursor.execute(operation=sql, params=param)
        except ProgrammingError as pe:
            cursor.close()
            cnx.close()
            print(str(pe))
            return dict(error='error in executing sql %s with param %s' % (sql, param))

        temp = cursor.fetchone()
        cursor.close()
        cnx.commit()
        cnx.close()

        if temp is None:
            temp = {}

        return temp