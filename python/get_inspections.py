import boto3
import requests
import json
import datetime

print('Starting at {}'.format(datetime.datetime.now()))
r = requests.get('https://data.cityofnewyork.us/resource/43nn-pn8j.json?$limit=300000')
r.status_code
data = r.json()
print('Finished retrieving json at {}'.format(datetime.datetime.now()))

s3 = boto3.resource('s3')

filename = str(str(datetime.datetime.now())+'.json')
obj = s3.Object('doh-inspection-storage',filename)
obj.put(Body=json.dumps(data))

print('Upload complete at {}'.format(datetime.datetime.now()))
