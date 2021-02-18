from DBUtils.PooledDB import PooledDB
import pymysql
from psycopg2 import pool
import pandas as pd
from sqlalchemy.engine import create_engine


class DBManager(object):
    def __init__(self, host, db=None, user=None, password=None, dbtype=None):
        if dbtype == 'mysql':
            driver_str = "mysql+pymysql"
            port = 3306
            self.pool = PooledDB(pymysql, mincached=1,
                                 maxcached=18, maxconnections=18,
                                 host=host,
                                 port=port,
                                 user=user,
                                 password=password,
                                 database=db
                                 )
        elif dbtype == 'postgresql':
            driver_str = "postgresql+psycopg2"
            port = 5432
            self.pool = pool.SimpleConnectionPool(2, 10, host=host, port=port,
                                                  user=user, password=password,
                                                  database=db, keepalives=1,
                                                  keepalives_idle=30, keepalives_interval=10,
                                                  keepalives_count=5)
        else:
            raise TypeError

        app_str = ""
        if user:
            app_str = user
            if password:
                app_str = app_str + ":" + password
            app_str = app_str + '@'
        print("%s//%s%s:%d/%s" % (driver_str, app_str, host, port, db))

        self.engine = create_engine("%s://%s%s:%d/%s" % (driver_str, app_str, host, port, db))
        self._insert_sql = "INSERT INTO {} ( {} ) VALUES ( {} ) "
        self._delete_sql = "DELETE FROM {} WHERE {} = '{}' "
        self._dbtype = dbtype

    def __del__(self):
        self.pool.closeall()

    def _get_connect(self):
        conn = None
        if self._dbtype == 'postgresql':
            conn = self.pool.getconn()
        elif self._dbtype == 'mysql':
            conn = self.pool.connection()
        cursor = conn.cursor()
        return conn, cursor

    def _close_connect(self, conn, cursor):
        cursor.close()
        if self._dbtype == 'postgresql':
            self.pool.putconn(conn)
        elif self._dbtype == 'mysql':
            conn.close()

    def _format(self, d):
        values = ','.join(map(lambda x: "'" + str(x) + "'", d.values()))
        keys = ','.join(d.keys())
        return keys, values

    def insert(self, d, tb):
        sql_ = self._insert_sql.format(tb, *self._format(d))
        conn, cursor = self._get_connect()
        ret = cursor.execute(sql_)
        conn.commit()
        self._close_connect(conn, cursor)
        return ret

    def delete(self, key, value, tb):
        sql_ = self._delete_sql.format(tb, key, value)
        conn, cursor = self._get_connect()
        ret = cursor.execute(sql_)
        conn.commit()
        self._close_connect(conn, cursor)
        return ret

    def delete_and_insert(self, key, d, tb):
        if key not in d:
            raise KeyError
        self.delete(key, d.get(key), tb)
        self.insert(d, tb)

    def query_df(self, sql):
        return pd.read_sql(sql, self.engine)

    def insert_df(self, df, tb, if_exists='append'):
        return df.to_sql(tb, self.engine, if_exists)
