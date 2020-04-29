import boto3
import pandas as pd

def run_update_master_table():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('doh-inspection-storage')
    f = bucket.objects.filter()
    latest = [obj.key for obj in sorted(f, key=lambda x: x.last_modified, reverse=True)][0]


    s3 = boto3.client('s3')
    with open('daily.csv', 'wb') as f:
        s3.download_fileobj('doh-inspection-storage', latest, f)

    df = pd.read_csv('daily.csv')

    df.rename(columns={'record_date':'last_seen'}, inplace=True)

    df['violation_code'] = df['violation_code'].apply(lambda x: '' if pd.isna(x) else x)
    df['key'] = df['camis'].astype(str) + df['inspection_date'] + df['violation_code']

    df.set_index('key', inplace=True)
    df['first_seen'] = df['last_seen'].copy()
    df.sort_values(['camis','inspection_date'], inplace=True)





"""INSERT INTO table_name (`camis`, `dba`, `boro`, `building`, `street`, `zipcode`, `phone`,
       `cuisine_description`, `inspection_date`, `action`, `violation_code`,
       `violation_description`, `critical_flag`, `score`, `grade`,
       `grade_date`, `last_seen`, `inspection_type`, `latitude`, `longitude`,
       `community_board`, `council_district`, `census_tract`, `bin`, `bbl`,
       `nta`, `key`, `first_seen`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
       %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""



(df.loc[0:'camis'], df.loc[0:'dba'], df.loc[0:'boro'], df.loc[0:'building'], df.loc[0:'street'], df.loc[0:'zipcode'],
df.loc[0:'phone'], df.loc[0:'cuisine_description'], df.loc[0:'inspection_date'], df.loc[0:'action'], df.loc[0:'violation_code'],
df.loc[0:'violation_description'], df.loc[0:'critical_flag'], df.loc[0:'score'], df.loc[0:'grade'],
df.loc[0:'grade_date'], df.loc[0:'last_seen'], df.loc[0:'inspection_type'], df.loc[0:'latitude'], df.loc[0:'longitude'],
df.loc[0:'community_board'], df.loc[0:'council_district'], df.loc[0:'census_tract'], df.loc[0:'bin'], df.loc[0:'bbl'],
df.loc[0:'nta'], df.loc[0:'key'], df.loc[0:'first_seen'])
