import boto3
import requests
import json
import datetime

r = requests.get('https://data.cityofnewyork.us/resource/43nn-pn8j.json?$limit=30')
r.status_code
data = r.json()

s3 = boto3.resource('s3')

filename = str(str(datetime.datetime.now())+'.json')
obj = s3.Object('doh-inspection-storage',filename)
obj.put(Body=json.dumps(data))
