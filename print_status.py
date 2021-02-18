import pandas as pd
import re
pd.set_option('display.max_rows', None)
from sqlalchemy.engine import create_engine
engine = create_engine("postgresql://jmz:'@10.191.22.230:5432/logs")
citys=[
   '北京市','上海市','天津市','重庆市', '南京市','杭州市','广州市','深圳市 ','郑州市','青岛市','苏州市','济南市','乌鲁木齐市','沈阳市','长春市','哈尔滨市'
]
citys_str = '\'' + '\',\''.join(citys) + '\''
df = pd.read_sql(F"select province , area ,tag, count(1) from poi where tag not like '|' and area in ({citys_str}) group by province , area , tag ", engine)
df['city'] = df.apply(lambda x : x['province'] + '-' + x['area'] , axis=1)
del df['province']
del df['area']

# 修复一个POI同时属于多个tag类别的情况
poi_fix = df[df['tag'].str.contains('\|')]
df = df.drop(poi_fix.index)

for idx , row in poi_fix.iterrows():
    for tag in row['tag'].split('|'):
        df = df.append(pd.Series({'tag':tag, 'count':row['count'],'city':row['city']}),ignore_index=True)
        
md_text = df.groupby(['city','tag'])['count'].sum().unstack("city").dropna(how='all').fillna(0).astype(int).to_markdown()
md_text = re.sub( r'(-+:)|(:-+)',':-:',md_text )
md_text = re.sub( r' ','',md_text )
print(md_text)
