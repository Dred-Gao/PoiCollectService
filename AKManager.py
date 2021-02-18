import sys
import redis
from configparser import ConfigParser
import datetime

# 加载配置
conf = ConfigParser()
conf.read("spider.conf", encoding='utf-8')


class AK_Manager:
    """
    每天0点定时任务,重置AK到Redis队列
    """

    def __init__(self, host=None, password=None):
        self.baidu_ak_list = [
            
        ]
        self.gaode_ak_list = [
          
        ]
        self.host = host if host else conf.get('redis', 'host')
        self.r = redis.Redis(host=self.host, port=6379, password=password)
        self.ak_db = conf.get("redis", "ak_db")
        self.data_source = conf.get("common", "data_source" )
        if self.data_source == 'baidu':
            self.ak_list = self.baidu_ak_list
        else:
            self.ak_list = self.gaode_ak_list

    def reset(self):
        """
        重新添加所有AK到Redis集合
        :return:
        """
        self.r.delete(self.ak_db)

        for ak in self.ak_list:
            self.r.sadd(self.ak_db, ak)

    def get_ak_from_db(self):
        """
        返回当前db里的所有AK
        :return:
        """
        return self.r.smembers(self.ak_db)

    def count_ak_from_db(self):
        """
        返回当前db剩余AK数量
        :return:
        """
        return self.r.scard(self.ak_db)


if __name__ == '__main__':

    akmanager = AK_Manager()

    if len(sys.argv) <= 1:
        print("请输入执行参数 1 - 打印剩余AK数量 0 - 重置AK队列")
    elif sys.argv[1] == '0':
        akmanager.reset()
        print(datetime.datetime.now() , "AK队列已重置")
    elif sys.argv[1] == '1':
        print("队列剩余AK数量: ", akmanager.count_ak_from_db())
    elif sys.argv[1] == '2':
        print("队列剩余AK: ", akmanager.get_ak_from_db())
