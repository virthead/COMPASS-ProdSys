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
from schedconfig.models import Filestable4

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.x_check_dump.log')

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
#    tasks_list = Task.objects.all().filter(name='dvcs2016P01-DDD')
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    for t in tasks_list:
        logger.info('Getting run numbers for task %s status merging finished and status x check no' % t.name)
        runs_list = Job.objects.filter(task=t).filter(status_merging_evntdmp='finished').filter(status_x_check_evntdmp='no').order_by('run_number').values_list('run_number', flat=True).distinct()
        logger.info('Got list of %s runs' % len(runs_list))
        
        for run_number in runs_list:
            logger.info('Getting all jobs of run number %s' % run_number)
            jobs_list_count = Job.objects.all().filter(task=t).filter(run_number=run_number).exclude(chunk_number_merging_evntdmp=-1).values_list('panda_id_merging_evntdmp', flat=True).distinct().count()
            logger.info(jobs_list_count)
            
            fSize_run = 0
            
            logger.info('Getting jobs with merging evntdmp status finished for task %s and run number %s' % (t.name, run_number))
            merging_jobs_list = Job.objects.filter(task=t).filter(run_number=run_number).filter(status_merging_evntdmp='finished').exclude(chunk_number_merging_evntdmp=-1).values('panda_id_merging_evntdmp', 'chunk_number_merging_evntdmp').distinct()
            logger.info('Got list of %s jobs' % len(merging_jobs_list))
            if len(merging_jobs_list) != jobs_list_count:
                logger.info('Not all jobs of run are ready for checking, skipping')
                continue
            
            for mj in merging_jobs_list:
                fname = 'evtdump%(prodSlt)s-%(runNumber)s.raw' % {'runNumber': run_number, 'prodSlt': t.prodslt}
                if format(mj['chunk_number_merging_evntdmp'], '03d') != '000':
                    fname = fname + '.' + format(mj['chunk_number_merging_evntdmp'], '03d')
                mj_check = Filestable4.objects.using('schedconfig').get(pandaid=mj['panda_id_merging_evntdmp'], type='output', lfn=fname)
                fSize_run = mj_check.fsize
                
                logger.info('Got merging job with filesize %s' % mj_check.fsize)
                logger.info('Going to get chunks with panda_id_merging_evntdmp=%s' % mj_check.pandaid)
                jobs_list = Job.objects.filter(panda_id_merging_evntdmp=mj_check.pandaid).values('panda_id')
                logger.info('Got list of %s jobs' % len(jobs_list))
                fSize_chunks = 0
                for j in jobs_list:
                    j_check = Filestable4.objects.using('schedconfig').get(pandaid=j['panda_id'], type='output', lfn='testevtdump.raw')
                    if j_check.fsize != 33:
                        fSize_chunks += j_check.fsize
            
            logger.info('File size of merged event dump of run %s and sum of file sizes in chunks: %s - %s' % (run_number, fSize_run, fSize_chunks))
            
            if fSize_run == fSize_chunks:
                logger.info('File sizes of merged files and run are equal')
                logger.info('Going to update x check status of jobs of run number %s to yes' % run_number)
                jobs_list_update = Job.objects.filter(task=t).filter(run_number=run_number).exclude(chunk_number_merging_evntdmp=-1).update(status_x_check_evntdmp='yes', status_castor_evntdmp='ready', date_updated=today)
            else:
                logger.info('File sizes of merged files and run are not equal')
                logger.info('Going to update x check status of jobs of run number %s to manual check is needed' % run_number)
                jobs_list_update = Job.objects.filter(task=t).filter(run_number=run_number).update(status_x_check_evntdmp='manual check is needed', date_updated=today)
             
    logger.info('done')

if __name__ == "__main__":
    sys.exit(main())
