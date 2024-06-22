import time
import mysql.connector

from ob_db.db_conf import ConfSecret
from ob_util.log_util import printl


DB_LAKE = ConfSecret.db_lake


def db_conn(read_only=False, retry=1):
    try:
        cnx = mysql.connector.connect(user=ConfSecret.db_conn_info['user'],
                                      password=ConfSecret.db_conn_info['pw'],
                                      host=ConfSecret.host,
                                      port=ConfSecret.db_conn_info['port'],
                                      allow_local_infile=True
                                      # ssl_ca=ssl_ca,
                                      )
        return cnx
    except mysql.connector.Error as err:
        printl("db connect error: ", err)
        if retry > 0:
            printl("waiting to connect again : ", retry)
            time.sleep(5)
            cnx = db_conn(read_only, retry=retry - 1)
            return cnx
        else:
            raise err


class Db:
    def __init__(self, cnx=None):
        if cnx:
            self.cnx = cnx
        else:
            self.cnx = None

        self.cursor = None
        self.stopwatch_start = None

    def connect(self, read_only=False, retry=1):
        self.cnx = db_conn(read_only=read_only, retry=retry)

    def get_cnx(self):
        return self.cnx

    def close(self):
        self.cnx.close()

    def commit(self):
        self.cnx.commit()

    def rollback(self):
        self.cnx.rollback()

    def stopwatch(self, msg=""):
        if self.stopwatch_start is None:
            self.stopwatch_start = time.time()
        else:
            print(msg, " : took ", time.time() - self.stopwatch_start)
            self.stopwatch_start = None
