import boto3
import pandas as pd

def run_update_master_table():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('doh-inspection-storage')
    f = bucket.objects.filter()
    latest = [obj.key for obj in sorted(f,  key=lambda x: x.last_modified,  reverse=True)][0]


    s3 = boto3.client('s3')
    with open('daily.csv',  'wb') as f:
        s3.download_fileobj('doh-inspection-storage',  latest,  f)

    df = pd.read_csv('daily.csv')

    df.rename(columns={'record_date':'last_seen'},  inplace=True)

    df['violation_code'] = df['violation_code'].apply(lambda x: '' if pd.isna(x) else x)
    df['key'] = df['camis'].astype(str) + df['inspection_date'] + df['violation_code']

    df.set_index('key',  inplace=True)
    df['first_seen'] = df['last_seen'].copy()
    df.sort_values(['camis', 'inspection_date'],  inplace=True)





"""INSERT INTO table_name (`camis`,  `dba`,  `boro`,  `building`,  `street`,  `zipcode`,  `phone`, 
       `cuisine_description`,  `inspection_date`,  `action`,  `violation_code`, 
       `violation_description`,  `critical_flag`,  `score`,  `grade`, 
       `grade_date`,  `last_seen`,  `inspection_type`,  `latitude`,  `longitude`, 
       `community_board`,  `council_district`,  `census_tract`,  `bin`,  `bbl`, 
       `nta`,  `key`,  `first_seen`) VALUES (%s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s, 
       %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s)"""



camis,  dba,  boro,  building,  street,  zipcode,  phone, 
cuisine_description,  inspection_date,  action,  violation_code, 
violation_description,  critical_flag,  score,  grade, 
grade_date,  last_seen,  inspection_type,  latitude,  longitude, 




community_board,  council_district,  census_tract,  bin,  bbl, 
nta,  key,  first_seen


zip(nutz.camis, nutz.dba, nutz.boro, nutz.building, 
nutz.street, nutz.zipcode, nutz.phone, 
nutz.cuisine_description, nutz.last_seen, nutz.latitude, 
nutz.longitudecommunity_board, nutz.council_district, 
nutz.census_tract, nutz.bin, nutz.bblnta, nutz.first_seen)




zip(restaurants.camis, restaurants.dba, restaurants.boro, restaurants.building, 
restaurants.street, restaurants.zipcode, restaurants.phone, 
restaurants.cuisine_description, restaurants.last_seen, restaurants.latitude, 
restaurants.longitudecommunity_board, restaurants.council_district, 
restaurants.census_tract, restaurants.bin, restaurants.bblnta, restaurants.first_seen)

%s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s)


zip(restaurants.dba, restaurants.inspection_date, restaurants.action, restaurants.violation_code, 
      restaurants.violation_description, restaurants.critical_flag, restaurants.score, restaurants.grade, 
      restaurants.grade_date, restaurants.last_seen, restaurants.inspection_type, restaurants.key, restaurants.first_seen)


'%s',  '%s',  '%s',  '%s',  '%s',  '%s',  '%s',  '%s',  '%s', 
'%s',  '%s',  '%s',  '%s',  '%s',  '%s',  '%s',  '%s',  '%s'





temp.camis, temp.dba, temp.boro, temp.building, temp.street, temp.zipcode, temp.phone, temp.
cuisine_description, temp.inspection_date, temp.action, temp.violation_code, temp.
violation_description, temp.critical_flag, temp.score, temp.grade, temp.
grade_date, temp.last_seen, temp.inspection_type, temp.latitude, temp.longitude, temp.
community_board, temp.council_district, temp.census_tract, temp.bin, temp.bbl, temp.
nta, temp.first_seen, temp.key, temp.inspection_id









cursor.execute("""UPDATE restaurants
    SET dba = new_restaurants.dba, 
     boro = new_restaurants.boro, 
     building = new_restaurants.building, 
     street = new_restaurants.street, 
     zipcode = new_restaurants.zipcode, 
     phone = new_restaurants.phone, 
     cuisine_description = new_restaurants.cuisine_description, 
     last_seen = new_restaurants.last_seen, 
     latitude = new_restaurants.latitude, 
     longitude = new_restaurants.longitude, 
     community_board = new_restaurants.community_board, 
     council_district = new_restaurants.council_district, 
     census_tract = new_restaurants.census_tract, 
     bin = new_restaurants.bin, 
     bbl = new_restaurants.bbl, 
     nta = new_restaurants.nta, 
     key = new_restaurants.key, 
    FROM new_restaurants
    WHERE restaurants.camis = new_restaurants.camis;""")


























violations.inspection_id, violations.violation_code, violations.score, violations.first_seen, violations.last_seen, violations.key


inspection_id = inspection_id.new_violations
violation_code = violation_code.new_violations
score = score.new_violations
first_seen =first_seen.new_violations
last_seen =last_seen.new_violations


violation_code = violation_code.new_violations
score = score.new_violations
first_seen = first_seen.new_violations
last_seen = last_seen.new_violations




inspections.camis, inspections.inspection_date, inspections.action, 
inspections.score, inspections.grade, inspections.grade_date, inspections.last_seen, 
inspections.inspection_type, inspections.inspection_id, inspections.first_seen


camis = inspections.camis
inspection_date = inspections.inspection_date
action = inspections.action
score = inspections.score
grade = inspections.grade
grade_date = inspections.grade_date
last_seen = inspections.last_seen
inspection_type = inspections.inspection_type
inspection_id = inspections.inspection_id
first_seen = inspections.first_seen





cursor = connection.cursor()

cursor.execute("""CREATE TABLE new_inspections AS (SELECT * FROM inspections) WITH NO DATA;""")


rows = zip(inspections.camis, inspections.inspection_date, inspections.action, 
inspections.score, inspections.grade, inspections.grade_date, inspections.last_seen, 
inspections.inspection_type, inspections.inspection_id, inspections.first_seen)

cursor.executemany("""INSERT INTO new_inspections ( camis,  inspection_date, 
action,  score,  grade,  grade_date,  last_seen,  inspection_type,  inspection_id,  first_seen)
        VALUES (%s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s);""",  rows)

connection.commit()

cursor.execute("""
    UPDATE new_inspections
    SET first_seen = inspections.first_seen
    FROM inspections
    WHERE new_inspections.camis = inspections.camis;
    """)

connection.commit()

cursor.execute("""UPDATE inspections
    SET camis = inspections.camis, 
    inspection_date = inspections.inspection_date, 
    action = inspections.action, 
    score = inspections.score, 
    grade = inspections.grade, 
    grade_date = inspections.grade_date, 
    last_seen = inspections.last_seen, 
    inspection_type = inspections.inspection_type
    FROM new_inspections
    WHERE inspections.inspection_id = new_inspections.inspection_id;""")

connection.commit()
cursor.close()











inspection_bin = []

for i in range(len(model)):
    current_type = model.loc[i,'inspection_type']
    current_score = model.loc[i,'score']
    current_camis = model.loc[i,'camis']
    current_action = model.loc[i,'action']
    current_time = model.loc[i,'time_til']
    current_event = model.loc[i,'event']
    
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
            previous_score = model.loc[i-1,'score']
            previous_camis = model.loc[i-1,'camis']
            
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
            previous_camis = model.loc[i-1,'camis']
            previous_score = model.loc[i-1,'score']
            
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

model['inspection_bin'] = inspection_bin

# bin_dummies = pd.get_dummies(model['inspection_bin'], drop_first=False)
# model = model.join(bin_dummies)
del(bin_dummies)
