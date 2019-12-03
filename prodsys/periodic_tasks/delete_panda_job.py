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

import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.delete_panda_job.log')

logger.info('Starting %s' % __file__)

logger.info('Setting environment for PanDA client')
aSrvID = None
os.environ["PANDA_URL_SSL"] = settings.PANDA_URL_SSL
os.environ["PANDA_URL"] = settings.PANDA_URL
os.environ["X509_USER_PROXY"] = settings.X509_USER_PROXY

logger.info('PanDA URL SSL: %s' % os.environ["PANDA_URL_SSL"])

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process(__file__, pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

def main():
    max_check_amount = 10000
    
    logger.info('Update jobs with status status_panda_job_deleted ready and panda_id=0 to finished')
    jobs_list = Job.objects.filter(status_panda_job_deleted='ready').filter(panda_id=0).update(status_panda_job_deleted='finished', date_updated=timezone.now())
    
    logger.info('Getting jobs with status status_panda_job_deleted ready')
    jobs_list = Job.objects.filter(status_panda_job_deleted='ready').order_by('panda_id')[:max_check_amount]
    logger.info('Got list of %s jobs' % len(jobs_list))
    
    i = 0
    for j in jobs_list:
        logger.info('Job %s of %s' % (i, max_check_amount))
        j_delete = []
        j_delete.append(j.panda_id)
    
        logger.info('Sending killJobs request to PanDA server for panda_id %s' % j.panda_id)
        s,o = Client.killJobs(j_delete,srvID=aSrvID)
        if s == 0:
            logger.info('Job was cancelled in PanDA')
            logger.info('Going to update status_panda_job_deleted to finished')
            j_update = Job.objects.filter(id=j.id).update(status_panda_job_deleted='finished', date_updated=timezone.now())
        else:
            logger.info('Something went wrong: %s' % o)
        
        i += 1
    
    logger.info('done')

if __name__ == "__main__":
    sys.exit(main())
