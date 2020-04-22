import boto3
import pandas as pd

s3 = boto3.resource('s3')
bucket = s3.Bucket('doh-inspection-storage')
f = bucket.objects.filter()
latest = [obj.key for obj in sorted(f, key=lambda x: x.last_modified, reverse=True)][0]
print(latest)

s3 = boto3.client('s3')
with open('testdl.csv', 'wb') as f:
    s3.download_fileobj('doh-inspection-storage', '2020-04-21 14:45:00.562534.csv', f)

df = pd.read_csv('testdl.csv')

df.head()

df.rename(columns={'record_date':'last_seen'}, inplace=True)

df['violation_code'] = df['violation_code'].apply(lambda x: '' if pd.isna(x) else x)
df['key'] = df['camis'].astype(str) + df['inspection_date'] + df['violation_code']

df.set_index('key', inplace=True)
df['first_seen'] = df['last_seen'].copy()
df.sort_values(['camis','inspection_date'], inplace=True)

df.head()
