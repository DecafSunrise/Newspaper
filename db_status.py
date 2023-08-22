import json
import psycopg2
import requests
import os
from dotenv import load_dotenv
load_dotenv()

## Set up env variables
database = os.getenv('DATABASE')
host = os.getenv('DBHOST')
port = os.getenv('DBPORT')
user = os.getenv('DBUSER')
password = os.getenv('DBPASSWORD')
SIGNAL_API_URL = os.getenv('SIGNAL_API_URL')
mynumber = os.getenv('mynumber')
newspaper_group = os.getenv('newspaper_group')

## Run SQL query
conn = psycopg2.connect(
      database=database, user=user, 
    password=password, host=host, port=port
  )

cursor = conn.cursor()
sql = '''SELECT count(*) FROM articles;'''
cursor.execute(sql)
results = cursor.fetchall()
conn.close()

## Assemble & Send Notification
payload = f"{results[0][0]:,} articles in database"

content = json.dumps({"message": payload, 
            "number": mynumber, 
            "recipients": [ newspaper_group ]})

requests.post(f"{SIGNAL_API_URL}v2/send", content)