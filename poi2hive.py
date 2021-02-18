import os
import sys
import pyhdfs
import pandas as pd
import shapely.wkt as wkt
from tempfile import TemporaryDirectory
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from ubd.dbconnect import postgres
from ubd.geofunc import GeohashOperator
from ubd.dbconnect import POI


def get_data(keywords: str) -> pd.DataFrame:
    """
    get poi related data from POI table

    Parameters
    ----------
    keywords : str
        search for a particular tag

    Returns
    ----------
    pd.DataFrame
        DataFrame read from POI


    Warning
    ----------
    Required for UBD package

    See Also
    ----------
    ubd.dbconnect.postgres : database  connector basing on sqlalchemy
    ubd.geofunc.GeohashOperator : geohash operation class
    ubd.dbconnect.POI : POI table class
    """
    db = postgres()
    go = GeohashOperator()
    Session = sessionmaker(bind=db.engine)
    session = Session()
    df_aoi = pd.read_sql(
        session.query(
            POI.name,
            func.ST_AsText(POI.aoi).label('geohash'),
            POI.province,
            POI.area,
            POI.district
        )
            .filter(POI.tag.match(f"%{keywords}%"))
            .filter(POI.aoi.isnot(None))
            .statement,
        session.bind)
    df_poi = pd.read_sql(
        session.query(
            POI.name,
            func.substr(POI.geohash, 1, 7).label('geohash'),
            POI.province,
            POI.area,
            POI.district
        )
            .filter(POI.tag.match(f"%{keywords}%"))
            .filter(POI.aoi.is_(None))
            .statement,
        session.bind)
    df_aoi['geohash'] = df_aoi['geohash'].apply(lambda x: list(go.polygon_geohasher(wkt.loads(x), 3, 7)))
    return pd.concat([df_aoi.explode('geohash'), df_poi])


def main(keywords: str, tag_type: str) -> None:
    """
    main function to get data from postgres and move it into hdfs

    Parameters
    ----------
    keywords : str
        tag string
    tag_type : str
        partition in hive

    See Also
    ----------
    get_data : get poi related data from POI table
    """
    res = get_data(keywords)

    with TemporaryDirectory() as dirname:
        # generate txt file
        res.to_csv(os.path.join(dirname, f'POI码表_{keywords}_每日扫描版.txt'), header=None, index=None, sep=',', mode='w')

        # move txt file into hdfs
        hdfs_path = f'/user/hive/warehouse/poi.db/code_aoi_geohash/tag_type={tag_type}/POI码表_{keywords}_每日扫描版.txt'
        hdfs = pyhdfs.HdfsClient(['10.244.16.101', '10.244.16.102'], user_name='hdfs')
        hdfs.copy_from_local(os.path.join(dirname, f'POI码表_{keywords}_每日扫描版.txt'), hdfs_path, overwrite=True)


def test():
    """
    pytest example
    """
    main('火车站', 'transportation')


if __name__ == '__main__':
    if 'http_proxy' in os.environ:
        del os.environ['http_proxy']
    if 'https_proxy' in os.environ:
        del os.environ['https_proxy']
    main(sys.argv[1], sys.argv[2])

