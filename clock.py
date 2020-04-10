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
