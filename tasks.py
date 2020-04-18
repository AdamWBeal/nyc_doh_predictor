import boto3
import requests
import json
import datetime

def run_gather_inspections():

    bucket_name = 'doh-inspection-storage'
    file_name = str(datetime.datetime.now())+'.csv'
    url = 'https://data.cityofnewyork.us/resource/43nn-pn8j.csv?$limit=1000000'

    print('Starting at: {}'.format(datetime.datetime.now()))

    r = requests.get(url, stream=True)

    with open(file_name, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)

    print('Finished retrieving csv at: {}'.format(datetime.datetime.now()))

    print('Beginning upload to S3 at: {}'.format(datetime.datetime.now()))

    s3 = boto3.client('s3')

    with open(file_name, "rb") as f:
        s3.upload_fileobj(f, bucket_name, file_name)

    print('Upload complete at: {}'.format(datetime.datetime.now()))
