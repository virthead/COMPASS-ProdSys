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
from schedconfig.models import Jobsactive4

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.resend_failed_jobs.log')

logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process(__file__, pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

def main():
    logger.info('Getting tasks with status resend failed')
    tasks_list = Task.objects.all().filter(status='resend failed')
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    for t in tasks_list:
        logger.info('Getting jobs in status failed for task %s' % t)
        jobs_list_count = Job.objects.all().filter(task=t).filter(attempt__lt=t.max_attempts).filter(status='failed').update(status='defined',
            status_merging_mdst=None, chunk_number_merging_mdst=-1, status_x_check='no',
            status_merging_histos=None,
            status_merging_evntdmp=None, chunk_number_merging_evntdmp=-1, status_x_check_evntdmp='no',
            status_castor_mdst=None, status_castor_histos=None, status_castor_evntdmp=None, 
            date_updated=timezone.now())
        
        logger.info('Going to update task status to sent')
        t_edit = Task.objects.get(id=t.id)
        t_edit.status = 'send'
        try:
            t_edit.save()
            logger.info('Finished processing task %s' % t)
        except Exception as e:
            logger.exception('%s (%s)' % (e.message, type(e)))

        
    logger.info('done')

if __name__ == "__main__":
    sys.exit(main())
