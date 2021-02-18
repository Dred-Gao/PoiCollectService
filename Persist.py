import redis
import time
import json
from utils.DBManager import DBManager
from configparser import ConfigParser

conf = ConfigParser()
conf.read("spider.conf", encoding='utf-8')

serialize_db = conf.get('common', 'serialize_db')


def persist():
    rs = None
    host = conf.get(serialize_db, 'host')
    dbname = conf.get(serialize_db, 'database')
    user = conf.get(serialize_db, 'username')
    password = conf.get(serialize_db, 'password')
    db = DBManager(host, db=dbname, user=user, password=password, dbtype='postgresql')

    pool = redis.ConnectionPool(host=conf.get('redis', 'host'),password=conf.get('redis','password'))
    r = redis.Redis(connection_pool=pool)
    db_src = conf.get('redis', 'result_db')
    db_obj = conf.get(serialize_db, 'table')

    while True:
        if r.llen(db_src) > 0:
            try:
                rs = r.lpop(db_src).decode()
                d = json.loads(rs)
                ret = db.delete_and_insert('uid', d, db_obj)
            except Exception as e:
                print('Persist Excetion')
                r.lpush(db_obj, rs)
        else:
            del db
            time.sleep(300)
            db = DBManager(host, db=dbname, user=user, password=password, dbtype='postgresql')


if __name__ == '__main__':
    persist()
