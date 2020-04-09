import boto3
import requests
import json
import datetime
import pandas as pd
import csv

# def run_gather_inspections():
#     # print('Starting at:                 {}'.format(datetime.datetime.now()))
#     r = requests.get('https://data.cityofnewyork.us/resource/43nn-pn8j.json?$limit=1')
#     data = r.json()
#     # print('Finished retrieving json at: {}'.format(datetime.datetime.now()))

# limit  = 2
# offset = 0
# for i in range(3):
#     r = requests.get('https://data.cityofnewyork.us/resource/43nn-pn8j.json?$limit={}$offset={}'.format(limit,offset))
#     print(r.json())
#     offset += limit



# r = requests.get('https://data.cityofnewyork.us/resource/43nn-pn8j.json?$limit=5&$offset=50')
# r = requests.get('https://data.cityofnewyork.us/resource/43nn-pn8j.csv?$limit=2')
#
# # df = pd.read_json(r.json())
# # print(r.json())
# df = pd.read_csv(r.content)


# CSV_URL = 'https://data.cityofnewyork.us/resource/43nn-pn8j.csv?$limit=2'


# with requests.Session() as s:
#     download = s.get(CSV_URL)
#
#     data = download.content.decode('utf-8')
#     s3 = boto3.resource('s3')
#
#     filename = str(str(datetime.datetime.now())+'.csv'
#     obj = s3.Object('doh-inspection-storage',filename)
#     obj.put(Body=json.dumps(data))
# 
#     print('Upload complete at:          {}'.format(datetime.datetime.now()))
#


r = requests.get('https://data.cityofnewyork.us/resource/43nn-pn8j.csv?$limit=2')
print(r.content)
