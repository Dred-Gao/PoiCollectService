# -*- coding: utf-8 -*-
"""
Created on 2020/8/10 14:59

@author: sun shaowen
"""
import queue
import geohash
import numpy as np
from itertools import product
from scipy.ndimage import convolve
from shapely.ops import cascaded_union
from shapely.geometry import box, Polygon
from shapely.geometry.base import BaseGeometry


class GeohashOperator(object):
    """
    Geohash操作类

    Methods
    ----------
    nearby_geohash(hash_string, desired=0)
        临近位置geohash块
    geohash_lonlac(geohash_code, location='s')
        查询geohash边界经纬度
    polygon_into_geohash(geo, accuracy=7)
        多边形分割成固定精度的geohashes， 按顺序输出
    smooth_polygon(polygon, data, precision=7, mode='dict', kernel=np.array([[1/9, 1/9, 1/9], [1/9, 1/9, 1/9], [1/9, 1/9, 1/9]]))
        平滑geohash矩阵
    geohash_to_polygon(geo, Geometry=True, sequence=True)
        geohash转矩形
    polygon_to_multi_length_geohashes(polygon, precision)
        几何图形分割固定精度的geohashes
    geohashes_to_polygon(geohashes)
        将多个geohash转成矩形
    polygon_geohasher(input_poly, start_precision, stop_precision, intersect=True)
        将几何图形分割成最少的geohashes
    """
    def __init__(self):
        self.__EVEN = 0
        self.__ODD = 1
        self.__BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
        self.__NEIGHBORS = [["14365h7k9dcfesgujnmqp0r2twvyx8zb", "238967debc01fg45kmstqrwxuvhjyznp"],
                     ["p0r21436x8zb9dcf5h7kjnmqesgutwvy", "bc01fg45238967deuvhjyznpkmstqrwx"],
                     ["238967debc01fg45kmstqrwxuvhjyznp", "14365h7k9dcfesgujnmqp0r2twvyx8zb"],
                     ["bc01fg45238967deuvhjyznpkmstqrwx", "p0r21436x8zb9dcf5h7kjnmqesgutwvy"]]
        self.__BORDERS = [["028b", "0145hjnp"], ["prxz", "bcfguvyz"], ["0145hjnp", "028b"], ["bcfguvyz", "prxz"]]

    def nearby_geohash(self, hash_string: str, desired: int = 0) -> str:
        """
        求解周边的geohash块编码

        Parameters
        ----------
        hash_string : str
            目标Geohash字符串，任意精度（字符串长度任意）
        desired : int, optional
            所求位置, 默认为0，即上方
            可选 BOTTOM: 0, TOP: 1, LEFT: 2, RIGHT: 3

        Returns
        ----------
        str
            返回相对应的Geohash字符串

        Example
        ---------
        >>> G = GeohashOperator()
        >>> G.nearby_geohash('wx4ervz', 0)
        wx4ervx

        求geohash字符串"wx4ervz"下方的geohash块编码为'wx4ervx'
        """
        hash_string = hash_string.lower()
        last_char = hash_string[-1]
        odd_even = self.__ODD if len(hash_string) % 2 == 1 else self.__EVEN
        base = hash_string[0:len(hash_string) - 1]
        if last_char in self.__BORDERS[desired][odd_even]:
            base = self.nearby_geohash(base, desired)
        return base + self.__BASE32[self.__NEIGHBORS[desired][odd_even].index(last_char)]

    def geohash_lonlac(self, geohash_code: str, location: str = 's') -> float:
        """
        求geohash字符串的边界经度/维度

        Parameters
        ----------
        geohash_code : str
            目标geohash字符串
        location : str, optional
            geohash字符串对应的栅格
            s: 最小纬度, n: 最大纬度, e: 最大经度, w: 最小经度

        Returns
        ----------
        float
            所求的最大(最小)的经(维)度

        Example
        ---------
        >>> G = GeohashOperator()
        >>> G.geohash_lonlac('wx4ervz', 'n'), G.geohash_lonlac('wx4ervz', 'e')
        39.979248046875, 116.3671875

        >>> G.geohash_lonlac('wx4ervz', 's'), G.geohash_lonlac('wx4ervz', 'w')
        39.977874755859375, 116.36581420898438

        分别求一个geohash字符串对应围栏的最大纬度、最大经度; 最小维度、最小经度
        """
        return geohash.bbox(geohash_code)[location]

    def polygon_into_geohash(self, geo: BaseGeometry, accuracy: int = 7) -> list:
        """
        将多边形切割成固定精度的多个geohash块，将其按照位置输出成矩阵

        Parameters
        ----------
        geo : shapely.geometry.base.BaseGeometry
            目标多边形
        accuracy : int, optional
            Geohash的精度，默认为7

        Returns
        ----------
        list
            分割出的geohash字符串列表

        Examples
        ----------
        >>> g = GeohashOperator()
        >>> p = Polygon([[116.40233516693117, 39.95442126877703], [116.40233516693117, 39.95744689749303], [116.4070386902313, 39.95744689749303], [116.4070386902313, 39.95442126877703]])
        >>> g.polygon_into_geohash(p)
        [['wx4g2f1', 'wx4g2f4', 'wx4g2f5', 'wx4g2fh', 'wx4g2fj'],
        ['wx4g2cc', 'wx4g2cf', 'wx4g2cg', 'wx4g2cu', 'wx4g2cv'],
        ['wx4g2c9', 'wx4g2cd', 'wx4g2ce', 'wx4g2cs', 'wx4g2ct'],
        ['wx4g2c3', 'wx4g2c6', 'wx4g2c7', 'wx4g2ck', 'wx4g2cm']]

        See Also
        ----------
        nearby_geohash : 求解周边的geohash块编码
        geohash_to_polygon : 将Geohash字符串转成矩形
        geohash_lonlac : 求geohash字符串的边界经度/维度
        """
        boundary = geo.bounds
        geo_list, line_geohash = [], []
        horizontal_geohash = vertical_geohash = geohash.encode(boundary[1], boundary[0], accuracy)
        while True:
            vertical_geohash_polygon = self.geohash_to_polygon(vertical_geohash)
            if geo.contains(vertical_geohash_polygon) or geo.intersects(vertical_geohash_polygon):
                line_geohash.append(vertical_geohash)
                vertical_geohash = self.nearby_geohash(str(vertical_geohash), 3)
            elif self.geohash_lonlac(vertical_geohash, 'w') < boundary[2]:
                vertical_geohash = self.nearby_geohash(str(vertical_geohash), 3)
            else:
                if line_geohash:
                    geo_list.append(line_geohash)
                    line_geohash = []
                horizontal_geohash = vertical_geohash = self.nearby_geohash(horizontal_geohash, 1)
                horizontal_geohash_polygon = self.geohash_to_polygon(horizontal_geohash)
                if not (geo.contains(horizontal_geohash_polygon) or geo.intersects(horizontal_geohash_polygon) or (
                        self.geohash_lonlac(horizontal_geohash, 's') < boundary[3])):
                    return geo_list[::-1]

    def smooth_polygon(self, polygon: BaseGeometry, data: dict, precision: int = 7, mode: str = 'dict',
                       kernel: np.array = np.array([[1 / 9, 1 / 9, 1 / 9], [1 / 9, 1 / 9, 1 / 9], [1 / 9, 1 / 9, 1 / 9]])) -> [dict, np.array]:
        """
        根据平滑矩阵平滑多边形内的多geohash数据

        Parameters
        ----------
        polygon : shapely.geometry.base.BaseGeometry
            目标围栏几何图形
        data : dict
            围栏内键为geohash字符串的字典
        precision : int, optional
            均摊后的geohash精确度，默认为7
        mode : str, optional
            返回格式，默认为字典(dict)，也可以输出numpy matrix(matrix)或matrix flatten后的结果(flatten)
        kernel : numpy.array, optional
            默认是[[1 / 9, 1 / 9, 1 / 9], [1 / 9, 1 / 9, 1 / 9], [1 / 9, 1 / 9, 1 / 9]]的平滑矩阵

        Returns
        ----------
        dict or numpy.array
            返回平滑后的数据，可为dict, numpy matrix或matrix flatten的结果

        Examples
        ----------
        >>> g = GeohashOperator()
        >>> p = Polygon([[116.40233516693117, 39.95442126877703], [116.40233516693117, 39.95744689749303],
        >>> [116.4070386902313, 39.95744689749303], [116.4070386902313, 39.95442126877703]])
        >>> data = {'wx4g2f1':1, 'wx4g2f4':1, 'wx4g2f5':1, 'wx4g2fh':6, 'wx4g2fj':1, 'wx4g2cc':1, 'wx4g2cf':1,
        >>> 'wx4g2cg':1, 'wx4g2cu':1, 'wx4g2cv':1, 'wx4g2c9':8, 'wx4g2cd':1, 'wx4g2ce':1, 'wx4g2cs':1, 'wx4g2ct':1,
        >>> 'wx4g2c3':1, 'wx4g2c6':1, 'wx4g2c7':1, 'wx4g2ck':1, 'wx4g2cm':0.1}
        >>> g.smooth_polygon(p, data)
        {'wx4g2f1': 1, 'wx4g2f4': 1, 'wx4g2f5': 2, 'wx4g2fh': 2, 'wx4g2fj': 2, 'wx4g2cc': 2, 'wx4g2cf': 1, 'wx4g2cg': 1,
        'wx4g2cu': 1, 'wx4g2cv': 1, 'wx4g2c9': 2, 'wx4g2cd': 1, 'wx4g2ce': 1, 'wx4g2cs': 0, 'wx4g2ct': 0, 'wx4g2c3': 2,
        'wx4g2c6': 1, 'wx4g2c7': 1, 'wx4g2ck': 0, 'wx4g2cm': 0}

        Raises
        ----------
        ValueError
            输出模式错误，只可为dict, matrix或flatten

        See Also
        ----------
        polygon_into_geohash : 将多边形切割成固定精度的多个geohash块，将其按照位置输出成矩阵
        """
        hash_matrix = np.array(self.polygon_into_geohash(box(*polygon.bounds), precision))
        tmp_data = dict(data)
        for key, value in tmp_data.items():
            diff = len(key) - precision
            if diff > 0:
                if key[:-diff] not in data:
                    data[key[:-diff]] = value
                else:
                    data[key[:-diff]] += value
            elif diff < 0:
                add_on = list(product(*[self.__BASE32] * diff))
                for i in add_on:
                    sub_key = key + i
                    del data[key]
                    data[sub_key] = value / 32 ** diff

        def f_(x):
            return data.get(x, 0)

        value_matrix = np.vectorize(f_)(hash_matrix)
        value_matrix_ = convolve(value_matrix, kernel)
        if mode == 'dict':
            return {A: B for A, B in zip(hash_matrix.flatten(), value_matrix_.flatten())}
        elif mode == 'matrix':
            return value_matrix_
        elif mode == 'flatten':
            return value_matrix_.flatten()
        else:
            raise ValueError('Function receive an unaccepted input, mode can only be dict, matrix or flatten')

    def geohash_to_polygon(self, geo: str, Geometry: bool = True, sequence: bool = True) -> [list, BaseGeometry]:
        """
        将Geohash字符串转成矩形

        Parameters
        ----------
        geo : str
            所求Geohash字符串
        Geometry : boolean, optional
            返回格式可以为Polygon或是点的列表，默认为Polygon
            True返回shapely.geometry.Polygon，False返回点的列表
        sequence : bool, optional
            返回时点的格式，默认为[lon, lat]。
            True为[lon, lat], False为[lat, lon]

        Returns
        ----------
        list or shapely.geometry.base.BaseGeometry
            geohash所对应的矩形

        Example
        ---------
        >>> G = GeohashOperator()
        >>> G.geohash_to_polygon('wx4ervz', True, True)
        POLYGON ((116.3658142089844 39.97787475585938, 116.3671875 39.97787475585938, 116.3671875 39.979248046875,
        116.3658142089844 39.979248046875, 116.3658142089844 39.97787475585938))

        求一个geohash字符串对应的
        """
        lat_centroid, lng_centroid, lat_offset, lng_offset = geohash.decode_exactly(geo)
        corner_1 = (lat_centroid - lat_offset, lng_centroid - lng_offset)
        corner_2 = (lat_centroid - lat_offset, lng_centroid + lng_offset)
        corner_3 = (lat_centroid + lat_offset, lng_centroid + lng_offset)
        corner_4 = (lat_centroid + lat_offset, lng_centroid - lng_offset)
        if sequence:
            corner_1, corner_2, corner_3, corner_4 = corner_1[::-1], corner_2[::-1], corner_3[::-1], corner_4[::-1]
        if Geometry:
            return Polygon([corner_1, corner_2, corner_3, corner_4, corner_1])
        else:
            return [corner_1, corner_2, corner_3, corner_4, corner_1]

    def polygon_to_multi_length_geohashes(self, polygon: Polygon, precision: int) -> tuple:
        """
        将目标几何图形切割成geohash，并输出包含与相交两个geohash字符串列表

        Parameters
        ----------
        polygon : shapely.geometry.Polygon
            目标几何图形
        precision : int, optional
            所求geohash经度

        Returns
        ----------
        tuple
            被目标几何图形包括的geohash字符串列表，与目标几何图形相交的geohash字符串列表

        Examples
        ----------
        >>> g = GeohashOperator()
        >>> p = Polygon([[116.40233516693117, 39.95442126877703], [116.40233516693117, 39.95744689749303],
        >>> [116.4070386902313, 39.95744689749303], [116.4070386902313, 39.95442126877703]])
        >>> g.polygon_to_multi_length_geohashes(p, 7)
        (
            {'wx4g2cd', 'wx4g2ce', 'wx4g2cf', 'wx4g2cg', 'wx4g2cs', 'wx4g2cu'},
            {'wx4g2c3', 'wx4g2c6', 'wx4g2c7', 'wx4g2c9', 'wx4g2cc', 'wx4g2ck', 'wx4g2cm', 'wx4g2ct', 'wx4g2cv',
            'wx4g2f1', 'wx4g2f4', 'wx4g2f5', 'wx4g2fh','wx4g2fj'}
        )

        See Also
        ----------
        geohash_to_polygon : 将Geohash字符串转成矩形
        """
        inner_geohashes = set()
        outer_geohashes = set()
        intersect_geohashes = set()
        testing_geohashes = queue.Queue()
        testing_geohashes.put(geohash.encode(polygon.exterior.xy[1][0], polygon.exterior.xy[0][0], precision))
        while not testing_geohashes.empty():
            current_geohash = testing_geohashes.get()
            if current_geohash not in inner_geohashes and current_geohash not in outer_geohashes:
                current_polygon = self.geohash_to_polygon(current_geohash)
                condition = polygon.intersects(current_polygon)
                if condition:
                    if polygon.contains(current_polygon):
                        inner_geohashes.add(current_geohash)
                    elif polygon.intersects(current_polygon):
                        intersect_geohashes.add(current_geohash)
                        outer_geohashes.add(current_geohash)
                    else:
                        outer_geohashes.add(current_geohash)
                    for neighbor in geohash.neighbors(current_geohash):
                        if neighbor not in inner_geohashes and neighbor not in outer_geohashes:
                            testing_geohashes.put(neighbor)
        return inner_geohashes, intersect_geohashes

    def geohashes_to_polygon(self, geohashes: list) -> Polygon:
        """
        将多个geohash合并并转化成多边形

        Parameters
        ----------
        geohashes : list
            geohash字符串的列表，精度可变

        Returns
        ----------
        shapely.geometry
            合并后的几何图形

        Examples
        ----------
        >>> g = GeohashOperator()
        >>> g.geohashes_to_polygon(['wx4g2f1', 'wx4g2f4', 'wx4g2f5'])

        返回一个unicom之后的集合对象
        """
        return cascaded_union([self.geohash_to_polygon(g) for g in geohashes])

    def __get_tmp_res(self, res: set, intersect_geohash: str, test_poly: BaseGeometry, tmp_precision: int,
                      stop_precision: int, intersect: bool):
        geohash_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k',
                        'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
        tmp_precision += 1
        for add_on in geohash_list:
            tmp_poly = self.geohash_to_polygon(intersect_geohash + add_on)
            if test_poly.contains(tmp_poly):
                res.add(intersect_geohash + add_on)
            elif test_poly.intersects(tmp_poly):
                if tmp_precision == stop_precision:
                    if intersect is True:
                        res.add(intersect_geohash + add_on)
                else:
                    res = res.union(
                        self.__get_tmp_res(res, intersect_geohash + add_on, test_poly, tmp_precision, stop_precision,
                                           intersect)
                    )
        return res

    def polygon_geohasher(self, input_poly: Polygon, start_precision: int, stop_precision: int,
                          intersect: bool = True) -> list:
        """
        几何图形转变精度geohash

        该方法接受输入的几何图形，开始精度和终止经度。
        过程中会先使用小精度的geohash进行切割，再逐步放大经度，最终将几何图形转成尽可能少的geohash块。

        Parameters
        ----------
        input_poly : shapely.geometry.base.BaseGeometry
            目标几何图形
        start_precision : int
            切割的开始经度
        stop_precision : int
            切割的终止经度
        intersect : bool, optional
            是否包括与目标几何图形相交的geohash块，默认为包括
            True：包括，False：不包括

        Returns
        ----------
        list
            切割后的geohash字符串列表

        Examples
        ----------
        >>> g = GeohashOperator()
        >>> p = Polygon([[116.40233516693117, 39.95442126877703], [116.40233516693117, 39.95744689749303],
        >>> [116.4070386902313, 39.95744689749303], [116.4070386902313, 39.95442126877703]])
        >>> g.polygon_geohasher(p, 2, 7)
        {'wx4g2c3', 'wx4g2c6', 'wx4g2c7', 'wx4g2c9', 'wx4g2cc', 'wx4g2cd', 'wx4g2ce', 'wx4g2cf', 'wx4g2cg', 'wx4g2ck',
        'wx4g2cm', 'wx4g2cs', 'wx4g2ct', 'wx4g2cu', 'wx4g2cv', 'wx4g2f1', 'wx4g2f4', 'wx4g2f5', 'wx4g2fh', 'wx4g2fj'}

        See Also
        ----------
        polygon_to_multi_length_geohashes : 将目标几何图形切割成geohash，并输出包含与相交两个geohash字符串列表
        """
        res = set()
        inner_geohashes_1, intersect_geohashes_1 = self.polygon_to_multi_length_geohashes(input_poly, start_precision)
        if start_precision == stop_precision:
            if intersect is True:
                inner_geohashes_1.update(intersect_geohashes_1)
            return inner_geohashes_1
        if len(intersect_geohashes_1) > 0:
            res = res.union(inner_geohashes_1)
            for intersect_geohashe in intersect_geohashes_1:
                res = res.union(
                    self.__get_tmp_res(res, intersect_geohashe, input_poly, start_precision, stop_precision, intersect))
        else:
            res = res.union(self.polygon_geohasher(input_poly, start_precision + 1, stop_precision, intersect))
        return res
