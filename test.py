import boto3
import requests
import json
import datetime
import pandas as pd
import csv

url = 'https://data.cityofnewyork.us/resource/43nn-pn8j.csv?$limit=5000'

response = requests.get(url, stream=True)
handle = open("insp.csv", "wb")
for chunk in response.iter_content(chunk_size=512):
    if chunk:  # filter out keep-alive new chunks
        handle.write(chunk)
