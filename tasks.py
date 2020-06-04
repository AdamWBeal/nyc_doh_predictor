import boto3
import requests
import json
import datetime
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd


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




def prepare_data(file):

    df = pd.read_csv(file)

    df = df[(df['inspection_type'].str.contains('Cycle')) | 
    (df['inspection_type'].str.contains('Pre')) |
        (pd.isna(df['inspection_type']))]

    df.rename(columns={'record_date': 'last_seen'}, inplace=True)
    df['violation_code'] = df['violation_code'].apply(lambda x: '' if pd.isna(x) else x)
    df['score'] = df['score'].apply(lambda x: '' if pd.isna(x) else x)

    df['inspection_date'] = pd.to_datetime(df['inspection_date'])
    df['last_seen'] = pd.to_datetime(df['last_seen'])
    df['grade_date'] = pd.to_datetime(df['grade_date'])
    df['first_seen'] = df['last_seen'].copy()

    df['key'] = df['camis'].astype(str) + df['inspection_date'].astype(str) + df['violation_code']
    df['key'] = df['key'].apply(lambda x: str(abs(hash(x)))[:12])
    df['inspection_id'] = df['camis'].astype(str) + df['inspection_date'].astype(str)
    df['inspection_id'] = df['inspection_id'].apply(lambda x: str(abs(hash(x)))[:12])

    df.sort_values(['camis','inspection_date'], inplace=True)
    df.drop_duplicates(subset='key',inplace=True)
    df.reset_index(inplace=True, drop=True)

    print("df processed")

    return df


def run_update_db():

    s3 = boto3.resource('s3')
    bucket = s3.Bucket('doh-inspection-storage')
    f = bucket.objects.filter()
    latest = [obj.key for obj in sorted(f, key=lambda x: x.last_modified, reverse=True)][0]
    print("Latest file is {}\n".format(latest))

    s3 = boto3.client('s3')
    with open('update.csv', 'wb') as f:
        s3.download_fileobj('doh-inspection-storage', latest, f)

    df = prepare_data('update.csv')

    engine = create_engine('postgresql://postgres:makers@localhost:5432/new_doh')

    print("Creating temp table for {}\n".format(latest))

    df.to_sql('temp', engine, if_exists="replace", index=False)
    with engine.connect() as con:
        con.execute('ALTER TABLE temp ADD PRIMARY KEY (key);')

    connection = psycopg2.connect(user="postgres",
                            password="makers",
                            host='localhost',
                            port="5432",
                            database="new_doh")

    cursor = connection.cursor()

    print("Updating Main\n")

    cursor.execute('''UPDATE main
    SET last_seen = temp.last_seen,
    score = temp.score,
    grade = temp.grade 
    FROM temp
    WHERE main.key = temp.key;''')

    connection.commit()

    print("Inserting into main\n")

    cursor.execute('''INSERT INTO main
    SELECT temp.camis, temp.dba, temp.boro, temp.building, temp.street, temp.zipcode, temp.phone, temp.cuisine_description, temp.inspection_date, temp.action, temp.violation_code, temp.violation_description, temp.critical_flag, temp.score, temp.grade, temp.grade_date, temp.last_seen, temp.inspection_type, temp.latitude, temp.longitude, temp.community_board, temp.council_district, temp.census_tract, temp.bin, temp.bbl, temp.nta, temp.first_seen, temp.key, temp.inspection_id
    FROM temp
    LEFT OUTER JOIN main ON (main.key = temp.key)
    WHERE main.key IS NULL;''')

    connection.commit()

    cursor.execute('''DROP TABLE temp;''')

    connection.commit()
    cursor.close()
    connection.close()


run_update_db()