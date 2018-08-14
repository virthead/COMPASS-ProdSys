#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import commands
import datetime
from django.conf import settings
import logging
from django.core.wsgi import get_wsgi_application
from django.db import DatabaseError, IntegrityError
from _mysql import NULL

max_send_amount = 100

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
getRotatingFileHandler(logger, 'periodic_tasks.test_send_jobs.log')

today = datetime.datetime.today()
logger.info('Starting %s' % __file__)

logger.info('Setting environment for PanDA client')
aSrvID = None
site = 'BW_COMPASS_MCORE'
os.environ["PANDA_URL_SSL"] = 'http://vm221-120.jinr.ru:25080/server/panda'
os.environ["PANDA_URL"] = 'https://vm221-120.jinr.ru:25443/server/panda'
os.environ["X509_USER_PROXY"] = settings.X509_USER_PROXY

logger.info('PanDA URL SSL: %s' % os.environ["PANDA_URL_SSL"])

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process(__file__, pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

def main():
    logger.info('Getting tasks with status send and running')
#    tasks_list = Task.objects.all().filter(Q(status='send') | Q(status='running'))
    tasks_list = Task.objects.all().filter(name='TestTaskBW')
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    for t in tasks_list:
        logger.info('Getting jobs in status defined or failed for task %s' % t)
        jobs_list_count = Job.objects.all().filter(task=t).count()
        if jobs_list_count > 50:
            jobs_list = Job.objects.all().filter(task=t).order_by('id')[:max_send_amount]
        else:
            jobs_list = Job.objects.all().filter(task=t).order_by('id')[:jobs_list_count]
        logger.info('Got list of %s jobs' % len(jobs_list))
        
        i = 0
        for j in jobs_list:
            if i >= max_send_amount:
                break
            
            logger.info('Going to send job %s of %s task' % (j.file, j.task.name))
        
            umark = commands.getoutput('uuidgen')
            datasetName = 'panda.destDB.%s' % umark 
            destName    = 'local' # PanDA will not try to move output data, data will be placed by pilot (based on schedconfig)
        
            job = JobSpec()
            job.taskID = j.task.id
            job.jobDefinitionID   = 0
            job.jobName           = 'hello world'
            job.transformation    = j.task.type # payload (can be URL as well)
            job.destinationDBlock = datasetName
            job.destinationSE     = destName
            job.currentPriority   = 2000
            job.prodSourceLabel   = 'test'
            job.computingSite     = site
            job.attemptNr = 1
            job.maxAttempt = 5
            job.sourceSite = 'BW_COMPASS_MCORE'
            job.VO = 'local'
        
            # logs, and all files generated during execution will be placed in log (except output file)
            job.jobParameters='python /u/sciteam/petrosya/panda/hello.py'
                    
            fileOLog = FileSpec()
            fileOLog.lfn = "log.job.log.tgz"
            fileOLog.destinationDBlock = job.destinationDBlock
            fileOLog.destinationSE     = job.destinationSE
            fileOLog.dataset           = job.destinationDBlock
            fileOLog.type = 'log'
            job.addFile(fileOLog)
    
            s,o = Client.submitJobs([job],srvID=aSrvID)
            logger.info(s)
            logger.info(o)
#             for x in o:
#                 logger.info("PandaID=%s" % x[0])
#                 today = datetime.datetime.today()
#                 
#                 if x[0] != 0 and x[0] != 'NULL':
#                     j_update = Job.objects.get(id=j.id)
#                     j_update.panda_id = x[0]
#                     j_update.status = 'sent'
#                     j_update.attempt = j_update.attempt + 1
#                     j_update.date_updated = today
#                 
#                     try:
#                         j_update.save()
#                         logger.info('Job %s with PandaID %s updated at %s' % (j.id, x[0], today)) 
#                     except IntegrityError as e:
#                         logger.exception('Unique together catched, was not saved')
#                     except DatabaseError as e:
#                         logger.exception('Something went wrong while saving: %s' % e.message)
#                 else:
#                     logger.info('Job %s was not added to PanDA' % j.id)
            i += 1
    
    logger.info('done')

if __name__ == "__main__":
    sys.exit(main())
