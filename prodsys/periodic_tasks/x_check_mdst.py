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

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job
from schedconfig.models import Jobsactive4, Jobsarchived4

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.x_check_mdst.log')

today = datetime.datetime.today()
logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process(__file__, pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

def main():
    logger.info('Getting tasks with status send, running and paused')
    tasks_list = Task.objects.all().filter(Q(status='send') | Q(status='running') | Q(status='paused'))
#    tasks_list = Task.objects.all().filter(name='dvcs2016P07t1_mu+_part1')
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    for t in tasks_list:
        logger.info('Getting run numbers for task %s status merging finished and status x check no' % t.name)
        runs_list = Job.objects.filter(task=t).filter(status_merging_mdst='finished').filter(status_x_check='no').order_by('run_number').values_list('run_number', flat=True).distinct()
        logger.info('Got list of %s runs' % len(runs_list))
        
        for run_number in runs_list:
            logger.info('Getting all jobs of run number %s' % run_number)
            jobs_list_count = Job.objects.all().filter(task=t).filter(run_number=run_number).values_list('panda_id_merging_mdst', flat=True).distinct().count()
            logger.info(jobs_list_count)
            
            nEvents_run = 0
            nEvents_chunks = 0
            
            logger.info('Getting jobs with merging mdst status finished for task %s and run number %s' % (t.name, run_number))
            merging_jobs_list = Job.objects.filter(task=t).filter(run_number=run_number).filter(status_merging_mdst='finished').values('panda_id_merging_mdst').distinct()
            logger.info('Got list of %s jobs' % len(merging_jobs_list))
            if len(merging_jobs_list) != jobs_list_count:
                logger.info('Not all jobs of run are ready for checking, skipping')
                continue
            
            for mj in merging_jobs_list:
                mj_check = Jobsarchived4.objects.using('schedconfig').get(pandaid=mj['panda_id_merging_mdst'])
                nEvents_run += mj_check.nevents
                
                logger.info('Got merging job with % events' % mj_check.nevents)
                logger.info('Going to get chunks with panda_id_merging_mdst=%s' % mj_check.pandaid)
                jobs_list = Job.objects.filter(panda_id_merging_mdst=mj_check.pandaid).values('panda_id')
                logger.info('Got list of %s jobs' % len(jobs_list))
                for j in jobs_list:
                    j_check = Jobsarchived4.objects.using('schedconfig').get(pandaid=j['panda_id'])
                    nEvents_chunks += j_check.nevents
            
            logger.info('Number of events in merged files of run %s and sum of events in chunks: %s - %s' % (run_number, nEvents_run, nEvents_chunks))
            
            if nEvents_run == nEvents_chunks:
                logger.info('Number of events in merged files and run are equal')
                logger.info('Going to update x check status of jobs of run number %s to yes' % run_number)
                jobs_list_update = Job.objects.filter(task=t).filter(run_number=run_number).update(status_x_check='yes', status_merging_histos='ready', status_merging_evntdmp='ready', status_castor_mdst='ready', date_updated=today)
            else:
                logger.info('Number of events in merged files and run are not equal')
                logger.info('Going to update x check status of jobs of run number %s to manual check is needed' % run_number)
                jobs_list_update = Job.objects.filter(task=t).filter(run_number=run_number).update(status_x_check='manual check is needed', date_updated=today)
        
    logger.info('done')

if __name__ == "__main__":
    sys.exit(main())
