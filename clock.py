# import boto3
# import requests
# import json
# import datetime
#
# from apscheduler.schedulers.blocking import BlockingScheduler
#
# sched = BlockingScheduler()
#
# @sched.scheduled_job('interval', minutes=15)
# def timed_job():
#     try:
#         print('Starting at:                 {}'.format(datetime.datetime.now()))
#         r = requests.get('https://data.cityofnewyork.us/resource/43nn-pn8j.json?$limit=500000')
#         data = r.json()
#         print('Finished retrieving json at: {}'.format(datetime.datetime.now()))
#
#         s3 = boto3.resource('s3')
#
#         filename = str(str(datetime.datetime.now())+'.json')
#         obj = s3.Object('doh-inspection-storage',filename)
#         obj.put(Body=json.dumps(data))
#
#         print('Upload complete at:          {}'.format(datetime.datetime.now()))
#     except Exception as e: print(e)
#
# sched.start()


from apscheduler.schedulers.blocking import BlockingScheduler
from rq import Queue
from worker import conn
from run import run_gather_inspections

import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

sched = BlockingScheduler()

q = Queue(connection=conn)

def gather_inspections():
  q.enqueue(run_gather_inspections)


sched.add_job(gather_inspections)
sched.add_job(gather_inspections, 'interval', minutes=5)
sched.start()
