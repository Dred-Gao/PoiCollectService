import math

class GisTransformer(object):
    """gis坐标转换类"""
    MCBAND = (12890594.86, 8362377.87, 5591021, 3481989.83, 1678043.12, 0)
    MC2LL = ([1.410526172116255e-8, 0.00000898305509648872, -1.9939833816331,
          200.9824383106796, -187.2403703815547, 91.6087516669843, - 23.38765649603339,
          2.57121317296198, -0.03801003308653, 17337981.2],
         [-7.435856389565537e-9, 0.000008983055097726239, -0.78625201886289,
          96.32687599759846, -1.85204757529826, -59.36935905485877, 47.40033549296737,
          -16.50741931063887, 2.28786674699375, 10260144.86],
         [-3.030883460898826e-8, 0.00000898305509983578, 0.30071316287616,
          59.74293618442277, 7.357984074871, -25.38371002664745, 13.45380521110908,
          -3.29883767235584, 0.32710905363475, 6856817.37],
         [-1.981981304930552e-8, 0.000008983055099779535, 0.03278182852591, 40.31678527705744,
          0.65659298677277, -4.44255534477492, 0.85341911805263, 0.12923347998204,
          -0.04625736007561, 4482777.06],
         [3.09191371068437e-9, 0.000008983055096812155, 0.00006995724062, 23.10934304144901,
          -0.00023663490511, -0.6321817810242, -0.00663494467273, 0.03430082397953,
          -0.00466043876332, 2555164.4],
         [2.890871144776878e-9, 0.000008983055095805407, -3.068298e-8, 7.47137025468032,
          -0.00000353937994, -0.02145144861037, -0.00001234426596, 0.00010322952773,
          -0.00000323890364, 826088.5])

    def __init__(self, old_gis_name, new_gis_name):
        """
        经纬度(谷歌高德):'wgs84'/  墨卡托:'webMercator'/ 火星坐标系(国测局):'gcj02' / 百度坐标系: 'bd09'

        PS:百度坐标系转换回其他坐标系有误差
        """
        self.x_pi = 3.14159265358979324 * 3000.0 / 180.0
        self.pi = 3.1415926535897932384626  # π   精度比math.pi 还高一些
        self.ee = 0.00669342162296594323  # 偏心率平方
        self.a = 6378245.0  # 长半轴

        func_name = old_gis_name + '_to_' + new_gis_name
        if hasattr(self, func_name):
            self.transform_func = getattr(self, func_name)

    def _out_of_china(self, lng, lat):
        """
        判断是否在国内，不在国内不做偏移
        :param lng:
        :param lat:
        :return:
        """
        return not (lng > 73.66 and lng < 135.05 and lat > 3.86 and lat < 53.55)

    def _transformlat(self, lng, lat):
        ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + \
              0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
        ret += (20.0 * math.sin(6.0 * lng * self.pi) + 20.0 *
                math.sin(2.0 * lng * self.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lat * self.pi) + 40.0 *
                math.sin(lat / 3.0 * self.pi)) * 2.0 / 3.0
        ret += (160.0 * math.sin(lat / 12.0 * self.pi) + 320 *
                math.sin(lat * self.pi / 30.0)) * 2.0 / 3.0
        return ret

    def _transformlng(self, lng, lat):
        ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
              0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
        ret += (20.0 * math.sin(6.0 * lng * self.pi) + 20.0 *
                math.sin(2.0 * lng * self.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lng * self.pi) + 40.0 *
                math.sin(lng / 3.0 * self.pi)) * 2.0 / 3.0
        ret += (150.0 * math.sin(lng / 12.0 * self.pi) + 300.0 *
                math.sin(lng / 30.0 * self.pi)) * 2.0 / 3.0
        return ret

    def wgs84_to_webMercator(self, lon, lat):
        """wgs84坐标 转 墨卡托坐标"""
        x = lon * 20037508.342789 / 180
        y = math.log(math.tan((90 + lat) * self.pi / 360)) / (self.pi / 180)
        y = y * 20037508.34789 / 180
        return x, y

    def gcj02_to_webMercator(self, x, y):
        """火星转墨卡托"""
        wgs84_x, wgs84_y = self.gcj02_to_wgs84(x, y)
        webMercator_x, webMercator_y = self.wgs84_to_webMercator(wgs84_x, wgs84_y)
        return webMercator_x, webMercator_y

    def webMercator_to_webMercator(self, x, y):
        return x, y

    def webMercator_to_wgs84(self, x, y):
        """墨卡托坐标 转 wgs84坐标"""
        lon = x / 20037508.34 * 180
        lat = y / 20037508.34 * 180
        lat = 180 / self.pi * (2 * math.atan(math.exp(lat * self.pi / 180)) - self.pi / 2)
        return lon, lat

    def gcj02_to_wgs84(self, lng, lat):
        """
        GCJ02(火星坐标系)转GPS84
        :param lng:火星坐标系的经度
        :param lat:火星坐标系纬度
        :return:
        """
        if self._out_of_china(lng, lat):
            return lng, lat
        dlat = self._transformlat(lng - 105.0, lat - 35.0)
        dlng = self._transformlng(lng - 105.0, lat - 35.0)
        radlat = lat / 180.0 * self.pi
        magic = math.sin(radlat)
        magic = 1 - self.ee * magic * magic
        sqrtmagic = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((self.a * (1 - self.ee)) / (magic * sqrtmagic) * self.pi)
        dlng = (dlng * 180.0) / (self.a / sqrtmagic * math.cos(radlat) * self.pi)
        mglat = lat + dlat
        mglng = lng + dlng
        new_x = lng * 2 - mglng
        new_y = lat * 2 - mglat
        return new_x, new_y

    def wgs84_to_gcj02(self, lng, lat):
        """
        WGS84转GCJ02(火星坐标系)
        :param lng:WGS84坐标系的经度
        :param lat:WGS84坐标系的纬度
        :return:
        """
        if self._out_of_china(lng, lat):  # 判断是否在国内
            return lng, lat
        dlat = self._transformlat(lng - 105.0, lat - 35.0)
        dlng = self._transformlng(lng - 105.0, lat - 35.0)
        radlat = lat / 180.0 * self.pi
        magic = math.sin(radlat)
        magic = 1 - self.ee * magic * magic
        sqrtmagic = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((self.a * (1 - self.ee)) / (magic * sqrtmagic) * self.pi)
        dlng = (dlng * 180.0) / (self.a / sqrtmagic * math.cos(radlat) * self.pi)
        mglat = lat + dlat
        mglng = lng + dlng
        return mglng, mglat

    def webMercator_to_gcj02(self, x, y):
        """墨卡托转火星"""
        wgs84_x, wgs84_y = self.webMercator_to_wgs84(x, y)
        gcj02_x, gcj02_y = self.wgs84_to_gcj02(wgs84_x, wgs84_y)
        return gcj02_x, gcj02_y

    def gcj02_to_bd09(self , lng , lat ):
        z = math.sqrt(lng * lng + lat * lat) + 0.00002 * math.sin(lat * self.x_pi)
        theta = math.atan2(lat, lng) + 0.000003 * math.cos(lng * self.x_pi)
        bd_lng = z * math.cos(theta) + 0.0065
        bd_lat = z * math.sin(theta) + 0.006
        return bd_lng , bd_lat

    def bd09_to_gcj02(self , lng , lat):
        x = lng - 0.0065
        y = lat - 0.006
        z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * self.x_pi)
        theta = math.atan2(y, x) - 0.000003 * math.cos(x * self.x_pi)
        gg_lng = z * math.cos(theta)
        gg_lat = z * math.sin(theta)
        return gg_lng , gg_lat

    def wgs84_to_bd09(self , lng , lat ):
        gcj02_lng , gcj02_lat = self.wgs84_to_gcj02(lng , lat)
        bd09_lng , bd09_lat = self.gcj02_to_bd09(gcj02_lng , gcj02_lat)
        return bd09_lng , bd09_lat

    def bd09_to_wgs84(self , lng , lat ):
        gcj02_lng, gcj02_lat = self.bd09_to_gcj02(lng, lat)
        wgs84_lng, wgs84_lat = self.gcj02_to_wgs84(gcj02_lng, gcj02_lat)
        return wgs84_lng, wgs84_lat

    def webMercator_to_bd09(self , lng , lat ):
        gcj02_lng, gcj02_lat = self.webMercator_to_gcj02(lng , lat )
        return self.gcj02_to_bd09(gcj02_lng, gcj02_lat)

    def bd09_to_webMercator(self , lng , lat):
        gcj02_lng, gcj02_lat = self.bd09_to_gcj02(lng , lat)
        return self.gcj02_to_bd09(gcj02_lng, gcj02_lat)


    def convert_MCT_2_BD09(self , lon, lat):
        """将墨卡托坐标转换成BD09
            Args:
                lon: float, 经度
                lat: float, 维度
            Returns:
                (x, y): tuple, 经过转换的x, y
        """
        ax = None

        # 获取常量ax
        for idx , mcband in enumerate(self.MCBAND):
            if lat >= mcband:
                ax = self.MC2LL[idx]
                break

        if ax is None:
            raise GISError("error lat:%s" % lat)

        e = ax[0] + ax[1] * abs(lon)
        i = abs(lat) / ax[9]
        aw = ax[2] + ax[3] * i + ax[4] * i * i + ax[5] * i * i * i +\
             ax[6] * i * i * i * i + ax[7] * i * i * i * i * i + ax[8] * i * i * i * i * i * i
        if lon < 0:
            e *= -1
        if lat < 0:
            aw *= -1
        return e, aw

    def parseGeo(self , mocator):
        results = []
        items = mocator.split("|")
        type_ = int(items[0])
        bound , aois_str = items[1] , items[2].strip(";")
        aois = aois_str.split(";")
        if type_ == 4:
            aois = [ aois_str.split("-")[1] for aoi in aois if aoi.split("-")[0] == '1']
        if type_ == 1:
            results.append(aois[0])
        else:
            for aoi in aois:
#                 if len(aoi) > 100:
#                     aoi = re.sub('/(-?[1-9]\d*\.\d*|-?0\.\d*[1-9]\d*|-?0?\.0+|0|-?[1-9]\d*),(-?[1-9]\d*\.\d*|-?0\.\d*[1-9]\d*|-?0?\.0+|0|-?[1-9]\d*)(,)/g',"$1,$2;" , aoi)
#                     results.append(aoi)
#                 else:
                aoi_rs = []
                aoi_points = aoi.split(",")
                for pidx in range(0, len(aoi_points), 2):
                    aoi_rs.append([float(aoi_points[pidx]),float(aoi_points[pidx+1])])
                results.append(aoi_rs)
        return results , bound
        
