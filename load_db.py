import pandas as pd
import json
## monkey patching the json loader
# For more info: https://github.com/pandas-dev/pandas/issues/26068
pd.io.json._json.loads = lambda s, *a, **kw: json.loads(s)

# from pgenv import *
import time
from datetime import datetime
import psycopg2
import sqlalchemy
import uuid

from sqlalchemy.dialects import postgresql
from sqlalchemy import Integer, Numeric, String, DateTime, create_engine
import os

from dotenv import load_dotenv
load_dotenv()
# print(os.listdir('/home/dan/articles'))

database = os.getenv('DATABASE')
host = os.getenv('DBHOST')
port = os.getenv('DBPORT')
user = os.getenv('DBUSER')
password = os.getenv('DBPASSWORD')

print(host)

print('Checking which articles we already have...')
try:
  conn = psycopg2.connect(
      database=database, user=user, 
    password=password, host=host, port=port
  )
  cursor = conn.cursor()
  sql = '''select * from "ArticleLoadStatus"'''
  cursor.execute(sql)
  results = cursor.fetchall()
  results = [x[0] for x in results]
  conn.close()
except:
  results = []

print('Connecting to dump new data...')
conn_string = fr'postgresql+psycopg2://{user}:{password}@{host}/{database}'
db = create_engine(conn_string)
conn = db.connect()


article_dumps = [x for x in os.listdir('/home/dan/articles') if x.endswith('.json')]

article_dumps = [x for x in article_dumps if x not in results]

print(f'{len(article_dumps)} json dumps to process...\n\n')

str_cols = ['link_hash','source_url', 'url', 'title', 'text', 'publish_date', 
 'summary', 'article_html', 'meta_description', 'meta_lang', 
 'canonical_link', 'source_domain', 'source_brand', 'source_description']

if len(article_dumps)>0:
    for articles in article_dumps:
        print('\t',articles)
        try:
          df = pd.read_json(os.path.join('/home/dan/articles', articles))
          
          df['docid'] = [f'doc://f{uuid.uuid4()}' for x in range(0, len(df))]
          
          df = df[['docid', 'link_hash', 'source_url', 'url', 'title', 'text', 'keywords', 'meta_keywords',
        'tags', 'authors', 'publish_date', 'summary', 'article_html',
        'meta_description', 'meta_lang', 'canonical_link',  'source_domain', 'source_brand',
        'source_description']]
          df['source_json'] = articles

          for x in str_cols:
            df[x] = df[x].astype(str)
        except:
          print("\t>> Whoops fuck something went wrong")
          continue
        # df['publish_date'] = pd.to_datetime(df['publish_date'])

        try:
          df.to_sql('articles', con=conn, if_exists='append', index=False, 
                              dtype={
                          'docid': String,
                          'link_hash': String,
                          'source_url':String,
                          'url':String,
                          'title':String,
                          'text':String,
                          'keywords':sqlalchemy.ARRAY(String),
                          'meta_keywords':sqlalchemy.ARRAY(String),
                          'tags':sqlalchemy.ARRAY(String),
                          'authors':sqlalchemy.ARRAY(String),
                          'publish_date': String,
                          'summary':String,
                          'article_html':String,
                          'meta_description':String,
                          'meta_lang':String,
                          'canonical_link':String,
                          'source_domain':String,
                          'source_brand':String,
                          'source_description':String,
                          'source_json':String
                          })
          df2 = pd.DataFrame([[articles, str(datetime.now())]]).rename(columns={0:'File', 1:'DateTime'})
          df2['DateTime'] = pd.to_datetime(df2['DateTime'])
          df2.to_sql('ArticleLoadStatus', con=conn, if_exists='append', index=False, 
                      dtype={'File': String(length=40), 'publish_date': DateTime})
        except Exception as e:
          print("\t\tError")
          print(e)
conn.close()
print(f"Done at {str(datetime.now())}")