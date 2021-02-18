import redis
from configparser import ConfigParser

# 加载配置
conf = ConfigParser()
conf.read("spider.conf", encoding='utf-8')


r = redis.Redis(conf.get("redis","host"),password=conf.get("redis","password"))
ak_db = conf.get('redis','ak_db')
task_db = conf.get('redis','task_db')
visit_db = conf.get('redis','visit_db')
result_db = conf.get('redis','result_db')
print("1. 剩余AK\t{ak}\n2. 任务队列\t{task}\n3. 存储队列\t{results}\n4. 已访问集合\t{visited}\n".format( ak=r.scard(ak_db) , task=r.llen(task_db) , results=r.llen(result_db) , visited=r.scard(visit_db)))
