#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import commands
from django.utils import timezone
from django.conf import settings
import logging
from django.core.wsgi import get_wsgi_application
from django.db import DatabaseError, IntegrityError
from _mysql import NULL
import random

import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job
from schedconfig.models import Jobsarchived4, Filestable4, MetaTable, JobParamsTable

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.delete_failed_panda_jobs.log')

logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process(__file__, pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

def main():
    logger.info('Getting tasks with status archived')
    tasks_list = Task.objects.all().filter(status='archived').filter(status_failed_jobs_deleted='no').order_by('id')
#    tasks_list = Task.objects.all().filter(id=7)
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    for t in tasks_list:
        max_delete_amount = 1000
        
        logger.info('Getting jobs in status failed for task %s' % t)
        jobs_list = Jobsarchived4.objects.using('schedconfig').filter(taskid=t.id).filter(jobstatus='failed')[:max_delete_amount]
        logger.info('Got list of %s jobs' % len(jobs_list))
        
        if len(jobs_list) == 0:
            logger.info('All failed jobs already deleted for task %s' % t)
            task_update = Task.objects.filter(id=t.id).update(status_failed_jobs_deleted='yes', date_updated=timezone.now())
            continue
        
        i = 0
        for j in jobs_list:
            logger.info('Processing job %s out of %s' % (i, max_delete_amount))
            
            logger.info('Going to delete panda job %s of %s task' % (j.pandaid, t))
            
            j_ft = Filestable4.objects.using('schedconfig').filter(pandaid=j.pandaid).delete()
            logger.info('Deleted from Filestable4')
            
            j_mt = MetaTable.objects.using('schedconfig').filter(pandaid=j.pandaid).delete()
            logger.info('Deleted from MetaTable')
            
            j_jpt = JobParamsTable.objects.using('schedconfig').filter(pandaid=j.pandaid).delete()
            logger.info('Deleted from JobParamsTable')
            
            j_ja = Jobsarchived4.objects.using('schedconfig').filter(pandaid=j.pandaid).delete()
            logger.info('Deleted from Jobsarchived4')
            
            i += 1
        
        break
    
    logger.info('done')

if __name__ == "__main__":
    sys.exit(main())
            