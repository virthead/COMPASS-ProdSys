#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import commands
from django.utils import timezone
import pytz
from django.conf import settings
import logging
from django.core.wsgi import get_wsgi_application
import subprocess
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Job
from schedconfig.models import Jobsarchived4

from utils import check_process, getRotatingFileHandler

max_check_amount = 3000

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.get_number_of_events_unscrambled.log')

today = timezone.now()
logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process(__file__, pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

def main():
    logger.info('Getting jobs of 2016 with number of events = -1 and number of events attempt > 0')
    jobs_list = Job.objects.all().filter(task__year=2016).filter(number_of_events=-1).filter(number_of_events_attempt__gt=0).order_by('id')
    logger.info('Got list of %s jobs' % len(jobs_list))
    
    i = 0
    for j in jobs_list:
        if i > max_check_amount:
            logger.info('Max check amount has reached, breaking')
            break
            
        logger.info('Going to get PanDA job for job %s' % j.file)
        try:
            j_panda = Jobsarchived4.objects.using('schedconfig').filter(pandaid=j.panda_id).filter(jobstatus='finished').get()
        except:
            logger.info('Job %s has not processed yet' % j.file)
            
        logger.info('Got number of events from PanDA job: %s, going to update the job' % j_panda.nevents)
        
        j_update = Job.objects.get(id=j.id)
        j_update.number_of_events = j_panda.nevents
        j_update.date_updated = today
        j_update.number_of_events_attempt += 1
        try:
            j_update.save()
            logger.info('Job %s was updated' % j_update.id) 
        except IntegrityError as e:
            logger.exception('Unique together catched, was not saved')
        except DatabaseError as e:
            logger.exception('Something went wrong while saving: %s' % e.message)
        
    logger.info('done')

if __name__ == "__main__":
    sys.exit(main())
