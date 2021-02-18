import geohash
import math
import json
import redis
import requests
import time
import logging
from shapely.geometry import Polygon
from configparser import ConfigParser
from utils.GisTransformer import GisTransformer
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# 加载配置
conf = ConfigParser()
conf.read("spider.conf", encoding='utf-8')

category = {}
for k, v in conf.items('category'):
    for item in v.split(','):
        category[item] = k

# 配置日志
logging.basicConfig(level=logging.WARN,
                    format="%(asctime)s %(name)s %(levelname)s %(pathname)s %(message)s ",
                    datefmt='%Y-%m-%d  %H:%M:%S %a ',
                    filename="spider.log"
                    )
logger = logging.getLogger(__name__)

region_str = "http://api.map.baidu.com/place/v2/search?city_limit=true&query={query}&scope=2&region={region}&output=json&ak={ak}&page_size=20&page_num={page_num}"
circular_str = "http://api.map.baidu.com/place/v2/search?coord_type=1&scope=2&true&location={lat},{lon}&radius={radius}&ak={ak}&output=json&page_size=20&page_num={page_num}"
box_str = "http://api.map.baidu.com/place/v2/search?coord_type=1&scope=2&query={query}&bounds={region}&output=json&ak={ak}&page_size=20&page_num={page_num}"
query_str = "http://api.map.baidu.com/place/v2/search?coord_type=1&output=json&page_size=20&scope=2&ak=%s&page_num=%d&query=%s&bounds=%s"
detail_str = "http://api.map.baidu.com/place/v2/detail?uid={uid}&output=json&scope=2&ak={ak}"
aoi_str = 'http://map.baidu.com/?reqflag=pcmap&coord_type=1&from=webmap&qt=ext&ext_ver=new&l=18&uid=%s'
gaode_region_poi = 'http://restapi.amap.com/v3/place/text?key={ak}&types={tag}&city={region}&offset=25&page={page_num}'
gaode_location_poi = 'http://restapi.amap.com/v3/place/polygon?key={ak}&types={tag}polygon={polygon}&offset=25&page={page_num}'
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Host": "map.baidu.com",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36}"
}
# 配置坐标系转换器
gis = GisTransformer('bd09', 'wgs84')

proxy_flag = conf.get('common', 'proxy') == 'true'
update_flag = conf.get('common', 'update') == 'true'

class Spider(object):
    """
    采集器主程序,分两个品种 1.uid采集  2.详情采集与AOI采集
    """

    def __init__(self):
        self.__r = redis.Redis(conf.get('redis', 'host'),password=conf.get('redis','password'))
        self.__task_db = conf.get('redis', 'task_db')
        # self.q_uids = 'uid'
        self.__visit_db = conf.get('redis', 'visit_db')
        self.__ak_db = conf.get('redis', 'ak_db')
        self.__result_db = conf.get('redis', 'result_db')

        self.__mode = conf.get('common', 'mode')  # grid / city

    def __get_ak(self):
        # 获得一个随机AK
        ak = self.__r.srandmember(self.__ak_db, 1)
        if ak:
            ak = ak[0].decode("utf8")
            return ak

    def __is_empty_ak(self):
        return self.__r.scard(self.__ak_db) == 0

    def __is_empty_task(self):
        return self.__r.llen(self.__task_db) == 0

    def __get_task(self):
        if not self.__is_empty_task():
            return self.__r.lpop(self.__task_db).decode('utf8')

    def __reset_task(self, keyword, region, mode='l'):
        if mode == 'l':
            self.__r.lpush(self.__task_db, region + '#' + keyword)
        else:
            self.__r.rpush(self.__task_db, region + '#' + keyword)

    def __remove_ak(self, ak):
        self.__r.srem(self.__ak_db, ak)

    def __is_visited(self, uid):
        if self.__r.sismember(self.__visit_db, uid):
            return True
        else:
            return False

    def __set_visited(self, uid):
        # 设置已访问
        if not self.__r.sismember(self.__visit_db, uid):
            self.__r.sadd(self.__visit_db, uid)
            return True
        else:
            return False

    @staticmethod
    def __request_url(url):
        if proxy_flag:
            proxy = requests.get('http://10.126.138.150:5010/get').text
            return requests.get(url, headers=headers, proxies={"https": proxy}).json()
        else:
            return requests.get(url, headers=headers).json()

    @staticmethod
    def get_aoi(uid):
        """
        给定UID,采集AOI和bound,转换坐标系
        :param uid:
        :return:  AOI列表
        """
        content = Spider.__request_url(aoi_str % uid).get(
            'content').get('geo')
        if content:
            wgs84_aois = []
            aois, bound = gis.parseGeo(content)  # 解析墨卡托坐标系
            # 围栏  坐标系转换:墨卡托-->百度-->wgs84
            for mocator in aois:
                bd_coord_aois = [list(gis.transform_func(*gis.convert_MCT_2_BD09(mct_x, mct_y))) for
                                 mct_x, mct_y in
                                 mocator]
                bd_coord_aois = [[round(lon, 6), round(lat, 6)] for lon, lat in bd_coord_aois]
                wgs84_aois.append(bd_coord_aois)

            if len(wgs84_aois) == 1:
                # 几乎100%是只有一个aoi,所以无需再套一层列表
                final_polygon = wgs84_aois[0]
            else:
                final_polygon = Polygon(wgs84_aois[0])
                for wgs84_aoi in wgs84_aois[1:]:
                    cur_polygon = Polygon(wgs84_aoi)
                    if cur_polygon.intersects(final_polygon):
                        final_polygon = final_polygon.difference(cur_polygon)
                    else:
                        final_polygon = final_polygon.union(cur_polygon)

            return Polygon(final_polygon).wkt
        return

    def __fix_tag(self, tag):
        if len(tag.split(";")) == 1:
            return (category.get(tag, '') + ';' + tag).strip(';')
        return tag

    def __get_attribute(self, uid):
        ak = self.__get_ak()
        attribute = ""
        if ak:
            url = detail_str.format(uid=uid, ak=ak)
            content = self.__request_url(url)['result']
            if isinstance(content.get('detail_info', 0), dict):
                content = content.get('detail_info')
                tag = content.get('tag', 0)
                if '旅游景点' in tag:
                    attribute = content.get('scope_grade', '')
                elif '医疗' in tag or '高等院校' in tag:
                    attribute = content.get('content_tag', '')
        return attribute

    def __push_result(self, result):
        if self.__set_visited(result['uid']):
            return self.__r.rpush(self.__result_db, json.dumps(result))

    def __parse_poi_info(self, uid, content):
        lon, lat = gis.transform_func(float(content['location']['lng']),
                                      float(content['location']['lat']))
        geohash_ = geohash.encode(lat, lon, 8)
        name = content['name']
        aoi = self.get_aoi(uid)
        province = content.get('province', '')
        area = content.get('city', '')
        district = content.get('area', '')
        tag = self.__fix_tag(content.get('detail_info', {}).get('tag', ''))
        telephone = content.get('telephone', '')

        # 指定的类型需要更相信的信息
        attribute = self.__get_attribute(uid) if tag and any([i in tag for i in ['医疗', '高等院校', '旅游景点']]) else None
        poi_info_dict = {
            'uid': uid,
            'poi': "POINT ( {} {} )".format(round(lon, 6), round(lat, 6)),
            'name': name,
            'geohash': geohash_,
            'province': province,
            'area': area,
            'district': district,
            'tag': tag,
            'telephone': telephone
        }
        if aoi:
            poi_info_dict['aoi'] = aoi
        if attribute:
            poi_info_dict['attribute'] = attribute
        return poi_info_dict

    def claw_by_region(self, keyword, region, page_num, page_nums):
        """
        行政区划采集器 , 仅支持单关键字检索
        优点：采集速度快
        缺点：返回POI数量不全，缺失问题
        :return:
        """
        if page_nums and page_num >= page_nums:
            return

        # 获得一个随机AK
        ak = self.__get_ak()

        url_format_str = box_str if region.find(",") >= 0 else region_str
        url = url_format_str.format(ak=ak, page_num=page_num, query=keyword, region=region)

        # 访问请求
        try:
            content = self.__request_url(url)
        except:
            self.__reset_task(keyword, region, mode='r')
            logger.error("Error Code : 001 . 区域检索访问异常: %s " % url)
            return

        if content['status'] == 0:
            total = content['total']
            if total == 0:  # 区域内没有目标
                logger.info("uid采集器: 区域无采集目标.")
                return
            elif total >= 400:  # 总数超过限制，区域分解递归
                if region.find(',') >= 0:
                    logger.warning("uid采集器: POI数量过大,进行递归采集 %s" % url)
                    min_lat, min_lon, max_lat, max_lon = region.split(',')
                    left_mid = str((float(min_lat) + float(max_lat)) / 2) + ',' + min_lon
                    mid_mid = str((float(min_lat) + float(max_lat)) / 2) + ',' + str(
                        (float(min_lon) + float(max_lon)) / 2)
                    mid_up = max_lat + ',' + str((float(min_lon) + float(max_lon)) / 2)
                    mid_down = min_lat + ',' + str((float(min_lon) + float(max_lon)) / 2)
                    right_mid = str((float(min_lat) + float(max_lat)) / 2) + ',' + max_lon
                    logger.info("uids: 总数%d, 递归左下区域 %s" % (total, p(min_lat, min_lon, mid_mid)))
                    self.claw_by_region(p(min_lat, min_lon, mid_mid), keyword, 0, None)
                    logger.info("uids: 总数%d, 递归左上区域 %s" % (total, p(left_mid, mid_up)))
                    self.claw_by_region(p(left_mid, mid_up), keyword, 0, None)
                    logger.info("uids: 总数%d, 递归右上区域 %s" % (total, p(mid_mid, max_lat, max_lon)))
                    self.claw_by_region(p(mid_mid, max_lat, max_lon), keyword, 0, None)
                    logger.info("uids: 总数%d, 递归右下区域 %s" % (total, p(mid_down, right_mid)))
                    self.claw_by_region(p(mid_down, right_mid), keyword, 0, None)
                else:
                    logger.warning(F"返回POI数量过多，请使用栅格采集模式 {region}")
                    # 自动启动滑动窗口采集模式，待改造
                return
            else:
                page_nums = math.ceil(total / 20.0) if page_nums is None else page_nums
                logger.info("uid采集器: 总数 %d, 当前页  %d/%d, " % (total, page_num + 1, page_nums))
                for result in content['results']:
                    try:
                        uid = result['uid']
                        # 检查是否访问过该目标
                        if not update_flag and self.__is_visited(uid):
                            continue
                        poi_info = self.__parse_poi_info(uid, result)

                    except Exception:
                        logger.info("uid采集器: 获得结果异常 %s" % url)
                        continue
                    else:
                        self.__push_result(poi_info)

                # 请求下一页
                self.claw_by_region(keyword, region, page_num + 1, page_nums)
        else:
            if content['status'] == 302:
                logger.info("uid采集器: 当前AK额度用尽,等待5s...")
                self.__remove_ak(ak)  # 删除 队列 ak
                self.__reset_task(keyword, region)  # 推送该失败box到队列前端
            elif content['status'] == 210:
                logger.warning("uid采集器: AK %s IP校验失败,等待5s..." % ak)
                self.__remove_ak(ak)
                self.__reset_task(keyword, region)
            elif content['status'] == 2:
                logger.warning("uid采集器: url 参数异常,忽略")
            elif content['status'] == 401:
                logger.info("uid采集器: 当前AK超过并发限制,等待5s...")
                self.__reset_task(keyword, region)
            else:
                logger.warning("uid采集器: 其他异常 状态码 %d " % content['status'])
            time.sleep(5)

    def claw_gaode_poi(self, keyword, region, page_num, page_nums):

        if page_nums and page_num >= page_nums:
            return

        # 获得一个随机AK
        ak = self.__get_ak()
        tag, query = keyword.split(';') if keyword.find(';') >= 0 else (None, keyword)

        url = gaode_region_poi.format(ak=ak, page_num=page_num, tag=tag, region=region)

        # 访问请求
        try:
            content = self.__request_url(url)
        except:
            self.__reset_task(keyword, region, mode='r')
            logger.error("Error Code : 001 . 区域检索访问异常: %s " % url)
            return

        if content['status'] == 0:
            count = content['count']
            if count == 0:  # 区域内没有目标
                logger.info("uid采集器: 区域无采集目标.")
                return
            elif count >= 1000:  # 总数超过限制，区域分解递归
                logger.warning("uid采集器: POI数量过大,请使用滑动窗口采集模式 %s" % url)
                # 自动启动滑动窗口采集模式，待改造
                return
            else:
                page_nums = math.ceil(count / 25.0) if page_nums is None else page_nums
                logger.info("uid采集器: 总数 %d, 当前页  %d/%d, " % (count, page_num + 1, page_nums))
                for result in content['pois']:
                    try:
                        uid = result['id']
                        # 检查是否访问过该目标
                        if self.__is_visited(uid):
                            continue
                        poi_info = self.__parse_poi_info(uid, result)

                    except Exception:
                        logger.info("采集器: 解析结果异常 %s" % url)
                        continue
                    else:
                        self.__push_result(poi_info)

                # 请求下一页
                self.claw_gaode_poi(keyword, region, page_num + 1, page_nums)
        else:
            if content['infocode'] == 10003:
                logger.info("uid采集器: 当前AK额度用尽,等待5s...")
                self.__remove_ak(ak)  # 删除 队列 ak
                self.__reset_task(keyword, region)  # 推送该失败box到队列前端
            elif content['infocode'] == 10005:
                logger.warning("uid采集器: AK %s IP校验失败,等待5s..." % ak)
                self.__remove_ak(ak)
                self.__reset_task(keyword, region)
            elif content['infocode'] == 10002:
                logger.warning("uid采集器: url 参数异常,忽略")
            elif content['infocode'] == 10014:
                logger.info("uid采集器: 当前AK超过并发限制,等待5s...")
                self.__reset_task(keyword, region)
            else:
                logger.warning("uid采集器: 其他异常 状态码 %d " % content['status'])
            time.sleep(5)

    def run_spider(self):
        while True:
            if self.__is_empty_ak():
                logger.info("主程序: AK已用尽,等待600s...")
                time.sleep(60)
                continue
            elif self.__is_empty_task():
                logger.info("主程序: 任务队列为空,等待600s...")
                time.sleep(60)
                continue
            task = self.__get_task()
            if task:
                region, keyword = task.split('#')
                self.claw_by_region(keyword, region, 0, None)


def p(*args):
    return ','.join(args)


def task():
    spider = Spider()
    spider.run_spider()


if __name__ == '__main__':
    executor = ProcessPoolExecutor(10)
    for i in range(10):
        executor.submit(task)
    executor.shutdown(wait=True)
    print("exit..")
