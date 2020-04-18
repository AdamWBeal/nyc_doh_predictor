import boto3
import requests
import json
import datetime

def run_gather_inspections():

    bucket_name = 'doh-inspection-storage'
    file_name = str(datetime.datetime.now())+'.json'
    url = 'https://data.cityofnewyork.us/resource/43nn-pn8j.json?$limit=1000000'

    print('Starting at: {}'.format(datetime.datetime.now()))

    # filename = 'data.csv'
    response = requests.get(url, stream=True)
    handle = open(file_name, 'wb')

    for chunk in response.iter_content(chunk_size=512):
        if chunk:
            handle.write(chunk)

    print('Finished retrieving csv at: {}'.format(datetime.datetime.now()))

    print('Beginning upload to S3 at: {}'.format(datetime.datetime.now()))

    s3 = boto3.client('s3')

    with open(file_name, "rb") as f:
        s3.upload_fileobj(f, bucket_name, file_name)

    print('Upload complete at: {}'.format(datetime.datetime.now()))
