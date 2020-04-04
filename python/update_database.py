from configparser import RawConfigParser
import json
import psycopg2
import sys, os
import numpy as np
import pandas as pd
import example_psql as creds
import pandas.io.sql as psql


conn_string = "host="+ creds.PGHOST +" port="+ "5432" +" dbname="+ creds.PGDATABASE +" user=" + creds.PGUSER \
+" password="+ creds.PGPASSWORD
conn=psycopg2.connect(conn_string)
print("Connected!")

# Create a cursor object
cursor = conn.cursor()


# def load_data(schema, table):
#
#     sql_command = "SELECT * FROM {}.{};".format(str(schema), str(table))
#     print (sql_command)
#
#     # Load the data
#     data = pd.read_sql(sql_command, conn)
#
#     print(data.shape)
#     return (data)
#
# rests = load_data('public', 'rest_table')
#
# rests = pd.DataFrame(rests)

word = 'THE'
# query = ("SELECT * FROM public.rest_table WHERE dba LIKE %s", (word,))

# df_model = pd.read_sql(query, conn)
# print(df_model)


cursor.execute("SELECT * FROM public.rest_table WHERE dba CONTAINS 'the'";)
cursor.fetchall()
