import redis
import sys, os
from configparser import ConfigParser
from utils.geohash import GeohashOperator
from shapely.geometry import MultiPolygon

conf = ConfigParser()
conf.read("spider.conf", encoding='utf-8')

r = redis.Redis(host=conf.get('redis', 'host'),password=conf.get('redis','password'))
geo = GeohashOperator()
len_geohash = int(conf.get('common','geohash_length'))


def push_task(region, query):
    r.rpush(conf.get('redis', 'task_db'), region + '#' + query)


def parse_city_to_sample_points(city, city_df):
    polygon = city_df.loc[city, 'aoi']
    if not polygon:
        print(F"{city} 不存在,请检查名称")
        exit(-1)
    if isinstance(polygon , MultiPolygon):
        geohashes = []
        for poly in polygon:
            geohashes.extend(geo.polygon_geohasher(poly, len_geohash,len_geohash, True))
    else:
        geohashes = geo.polygon_geohasher(polygon, len_geohash,len_geohash ,True)

    geohash_polygon = [geo.geohash_to_polygon(geohash, False)[:4:2] for geohash in geohashes]
    geohash_box_str = [','.join(map(str, left_down[::-1])) + "," + ','.join(map(str, right_top[::-1])) for
                       left_down, right_top in
                       geohash_polygon]
    return geohash_box_str


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("参数错误")
        exit(0)

    region, query = sys.argv[1], sys.argv[2]

    mode = conf.get('common', 'mode')
    assert mode in ('city', 'grid')
    # 城市检索模式
    if mode == "city":
        if region == '全国':
            citys = list(zip(*conf.items('city')))[0]
            for city in citys:
                push_task(city, query)
                print("push %s region to queue : %s" % (city, query))
        else:
            push_task(region, query)
            print("push %s region to queue : %s" % (region, query))
    # 栅格检索模式
    else:
        city_file = conf.get("common", "city_file")
        if city_file and os.path.exists(city_file) and os.path.isfile(city_file):
            import pandas as pd
            from shapely.wkt import loads

            city_df = pd.read_csv(city_file, sep=":", names=['label', 'aoi'])
            city_df['aoi'] = city_df['aoi'].map(lambda x: loads(x))
            city_df['city'] = city_df['label'].map(lambda x: x.split("|")[0].split("_")[1])
            city_df['prov'] = city_df['label'].map(lambda x: x.split("|")[0].split("_")[0])
            prov_city_dict = city_df[['prov', 'city']].groupby("prov").apply(lambda x: x['city'].to_list()).to_dict()
            city_df.set_index('city', inplace=True)

            if region == '全国':
                citys = list(zip(*conf.items('city')))[0]
                for city in citys:
                    city_sample_points = parse_city_to_sample_points(city, city_df)
                    for city_sample_point in city_sample_points:
                        push_task(city_sample_point, query)
                    print("push %s region to queue : %s" % (city, query))
            else:
                if region in prov_city_dict:
                    print(region)
                    for city in prov_city_dict[region]:
                        print("  +++" , city)
                        city_sample_points = parse_city_to_sample_points(city, city_df)
                        for city_sample_point in city_sample_points:
                            push_task(city_sample_point, query)
                else:
                    print(region)
                    city_sample_points = parse_city_to_sample_points(region, city_df)
                    for city_sample_point in city_sample_points:
                        push_task(city_sample_point, query)
                print("push %s region to queue : %s" % (region, query))
        else:
            print("spider.conf=>[common] city_file 存在错误")
            exit(-1)

