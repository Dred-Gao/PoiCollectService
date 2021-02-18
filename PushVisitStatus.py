import redis
import pandas as pd
from sqlalchemy.engine import create_engine
from configparser import ConfigParser
from utils.DBManager import DBManager

# 加载配置
conf = ConfigParser()
conf.read("spider.conf", encoding='utf-8')

serialize_db = conf.get('common', 'serialize_db')
visit_db = conf.get("redis", "visit_db")
redis_host = conf.get("redis", "host")
host = conf.get(serialize_db, "host")
dbname = conf.get(serialize_db, "database")
user = conf.get(serialize_db, "username")
password = conf.get(serialize_db, "password")
r = redis.Redis(redis_host, port=6379)
db = DBManager(host, db=dbname, user=user, password=password, dbtype='postgresql')
engine = db.engine

visited_uid = pd.read_sql("select uid from poi", engine, chunksize=10000)

r.delete(visit_db)

for batch in visited_uid:
    r.sadd(visit_db, *batch['uid'].to_list())
print("{} uid push to {} set from redis".format(serialize_db, visit_db))
