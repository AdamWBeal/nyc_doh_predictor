from apscheduler.schedulers.blocking import BlockingScheduler
from rq import Queue
from worker import conn
from tasks import run_gather_inspections

import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

sched = BlockingScheduler()

q = Queue(connection=conn)

def gather_inspections():
  q.enqueue(run_gather_inspections)

sched.add_job(gather_inspections, 'cron', hour=14, misfire_grace_time=60*3)

sched.start()
