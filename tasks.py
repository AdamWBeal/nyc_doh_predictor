import boto3
import requests
import json
import datetime
# import psycopg2
# import sqlalchemy
# from sqlalchemy import create_engine
from lifelines import CoxPHFitter
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


def retrieve_latest():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('doh-inspection-storage')
    f = bucket.objects.filter()
    latest = [obj.key for obj in sorted(f, key=lambda x: x.last_modified, reverse=True)][0]

    s3 = boto3.client('s3')
    with open('update.csv', 'wb') as f:
        s3.download_fileobj('doh-inspection-storage', latest, f)

def prepare_data(file):

    df = pd.read_csv(file)

    df = df[['camis', 'dba', 'building', 'street', 'boro', 'inspection_date', 'action', 'score','inspection_type','record_date']]
    df = df[df['inspection_date']!='1900-01-01T00:00:00.000']
    df = df[(df['inspection_type'].str.contains('Cycle')) | 
    (df['inspection_type'].str.contains('Pre')) |
           (pd.isna(df['inspection_type']))]

    df.rename(columns={'record_date': 'last_seen'}, inplace=True)
    df['last_seen'] = pd.to_datetime(df['last_seen'])
    df['inspection_date'] = pd.to_datetime(df['inspection_date'])

    df['inspection_id'] = df['camis'].astype(str) + df['inspection_date'].astype(str)
    df['rest_label'] = df['dba'] + ', ' + df['building'] + ' ' + df['street'] + ', ' + df['boro']

    df['rest_label'] = df['rest_label'].where(pd.notnull(df['rest_label']), df['dba'])

    df.drop(columns=['dba','building','street','boro'], inplace=True)

    df.sort_values(['camis','inspection_date'], inplace=True)
    df.drop_duplicates(subset='inspection_id',inplace=True)
    df.reset_index(inplace=True, drop=True)


    df['next_grade'] = df['inspection_date'].shift(-1)
    df.loc[df.drop_duplicates("camis", keep='last').index,'next_grade'] = pd.NaT

    df['time_til'] = df['next_grade'] - df['inspection_date']
    df['event'] = df['time_til'].apply(lambda x: 1 if pd.notnull(x) else 0)
    df.reset_index(inplace=True, drop=True)

    df['time_til'] = df['time_til'].dt.days
    df['time_til'] = df['time_til'].where(pd.notnull(df['time_til']), (df['last_seen'] - df['inspection_date']).dt.days)
    df = df[df['time_til'] < 500]
    df['score'] = df['score'].apply(lambda x: float(x))
    df.reset_index(drop=True, inplace=True)

    print('Cleaned')
    inspection_bin = []

    for i in range(len(df)):
        current_type = df.loc[i,'inspection_type']
        current_score = df.loc[i,'score']
        current_camis = df.loc[i,'camis']
        current_action = df.loc[i,'action']
        current_time = df.loc[i,'time_til']
        current_event = df.loc[i,'event']
        
        if 'losed' in current_action:
            inspection_bin.append('was_closed')
            
        elif 're-open' in current_action:
            inspection_bin.append('re-opened')
            
        elif 'Cycle' in current_type:
            if 'Initial' in current_type:
                if (current_score < 14) & (current_event == 1) & (current_time < 300):
                    inspection_bin.append('cyc_init_1')
                elif current_score < 14: # This event shouldn't be able to occur
                    inspection_bin.append('cyc_init_0')
                elif current_score > 13:
                    inspection_bin.append('cyc_init_1')
                    
            elif 'Re-' in current_type:
                previous_score = df.loc[i-1,'score']
                previous_camis = df.loc[i-1,'camis']
                
                if current_camis == previous_camis:
                    if previous_score < 14:
                        inspection_bin.append('cyc_re_0')
                    elif previous_score > 13 and previous_score < 28:
                        inspection_bin.append('cyc_re_1')
                    elif previous_score > 27:
                        inspection_bin.append('cyc_re_2')
                else:
                    inspection_bin.append('missing_prior_cycle')
            else:
                inspection_bin.append('other_cycle')
                
                
        elif 'Pre-' in current_type:
            if 'Initial' in current_type:
                if (current_score < 14) & (current_event == 1) & (current_time < 300):
                    inspection_bin.append('pre_init_1')
                elif current_score < 14:
                    inspection_bin.append('pre_init_0')
                elif current_score > 13:
                    inspection_bin.append('pre_init_1')
                    
            elif 'Re-' in current_type:
                previous_camis = df.loc[i-1,'camis']
                previous_score = df.loc[i-1,'score']
                
                if current_camis == previous_camis:
                    if previous_score < 14:
                        inspection_bin.append('pre_re_0')
                    elif previous_score > 13 and previous_score < 28:
                        inspection_bin.append('pre_re_1')
                    elif previous_score > 27:
                        inspection_bin.append('pre_re_2')
                else:
                    inspection_bin.append('missing_prior_pre')
            else:
                inspection_bin.append('other_pre')
        else:
            inspection_bin.append('other')
            

    inspection_bin = pd.Series(inspection_bin)

    df['inspection_bin'] = inspection_bin
    del(inspection_bin)
    print('Categorized')

    return df


def fit_model():
    model = prepare_data('update.csv')
    cph = CoxPHFitter()
    cph.fit(model[['time_til','event','inspection_bin','score']], duration_col='time_til', event_col='event', strata=['inspection_bin'])
    print('Model fit')
    censored_subjects = model.loc[~model['event'].astype(bool)]
    censored_subjects_last_obs = censored_subjects['time_til']
    
    unconditioned_sf = cph.predict_survival_function(censored_subjects)
    print('Uncond')
    conditioned_sf = unconditioned_sf.apply(lambda c: (c / c.loc[model.loc[c.name, 'time_til']]).clip_upper(1))
    print('Cond')

    conditioned_sf.columns = censored_subjects.index

    predictions = censored_subjects[['camis','rest_label','inspection_date','time_til']].join(conditioned_sf.T)

    return predictions

