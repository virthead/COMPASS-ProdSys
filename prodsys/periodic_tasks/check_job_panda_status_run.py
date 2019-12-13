#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import commands
from django.utils import timezone
import pytz
from django.conf import settings
import logging
from django.core.wsgi import get_wsgi_application
from django.db import DatabaseError, IntegrityError
from _mysql import NULL
import argparse

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job
from schedconfig.models import Jobsactive4, Jobsarchived4

from utils import check_process, getRotatingFileHandler

parser = argparse.ArgumentParser()
parser.add_argument('-t', '--task', type=int, required=True)
parser.add_argument('-r', '--run-number', type=int, required=True)
args = parser.parse_args()

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.check_job_panda_status_%s_%s.log' % (args.task, args.run_number))

today = timezone.now()
logger.info('Starting %s %s %s' %( __file__, args.task, args.run_number))

def main():
    logger.info('Getting task object with id %s' % args.task)
    t = Task.objects.get(id=args.task)
    
    logger.info('Getting jobs with status send and running for task number %s and run number %s' % (args.task, args.run_number))
    jobs_list = Job.objects.filter(task=t).filter(run_number=args.run_number).filter(Q(status='sent') | Q(status='running')).order_by('run_number').values()
    logger.info('Got list of %s jobs' % len(jobs_list))
    if len(jobs_list) == 0:
        sys.exit()
    
    logger.info('Getting jobs from PanDA table')
    jobs_list_panda = Jobsarchived4.objects.using('schedconfig').filter(taskid=args.task).filter(jobname__istartswith='%s-%s--%s-' % (t.production, t.year, args.run_number)).filter(Q(jobstatus='closed') | Q(jobstatus='cancelled') | Q(jobstatus='finished') | Q(jobstatus='failed')).values()
    logger.info('Got list of %s jobs' % len(jobs_list_panda))
    
    for j in jobs_list:            
        for p in jobs_list_panda:
            if p['pandaid'] == j['panda_id'] and p['jobstatus'] != j['status']:
                logger.info('Status in PanDA is %s, status in ProdSys is %s' % (p['jobstatus'], j['status']))
                
                today = timezone.now()
                j_update = Job.objects.get(panda_id=p['pandaid'])
                j_update.date_updated = today
                
                if p['jobstatus'] == 'cancelled':
                    j_update.status = 'failed'
                    try:
                        j_update.save()
                        logger.info('Job %s with PandaID %s updated' % (j_update.id, j_update.panda_id)) 
                    except IntegrityError as e:
                        logger.exception('Unique together catched, was not saved')
                    except DatabaseError as e:
                        logger.exception('Something went wrong while saving: %s' % e.message)
                    
                if p['jobstatus'] == 'closed':
                    j_update.status = 'failed'
                    try:
                        j_update.save()
                        logger.info('Job %s with PandaID %s updated' % (j_update.id, j_update.panda_id)) 
                    except IntegrityError as e:
                        logger.exception('Unique together catched, was not saved')
                    except DatabaseError as e:
                        logger.exception('Something went wrong while saving: %s' % e.message)
                
                if p['jobstatus'] == 'finished' or p['jobstatus'] == 'failed':
                    logger.info('Going to update status of job %s from %s to %s' % (j_update.file, j['status'], p['jobstatus']))
                
                    j_update.status = p['jobstatus']
                
                    if p['jobstatus'] == 'failed':
                        # refer to pilot's COMPASSExperiment PilotErrors for more details
                        if p['piloterrorcode'] == 1235 or p['piloterrorcode'] == 1236 or p['piloterrorcode'] == 1237 or p['piloterrorcode'] == 1243 or \
                            p['piloterrorcode'] == 1245 or p['piloterrorcode'] == 1247 or p['piloterrorcode'] == 1251 or p['piloterrorcode'] == 1253:
                            logger.info('%s, job status will be updated to manual check is needed' % p['piloterrordiag'])
                            j_update.status = 'manual check is needed'
                        if p['piloterrorcode'] == 1165 and p['piloterrorcode'] == 'Expected output file testevtdump.raw does not exist':
                            logger.info('Expected output file testevtdump.raw does not exist, job status will be updated to manual check is needed')  
                            j_update.status = 'manual check is needed'
                    
                    if p['jobstatus'] == 'finished':
                        if t.type == 'test production' or t.type == 'mass production' or t.type == 'technical production' or t.type == 'MC reconstruction':
                            j_update.status_merging_mdst = 'ready'
                    
                        if t.type == 'DDD filtering':
                            j_update.status_merging_evntdmp = 'ready'
                        
                        if t.type == 'MC generation':
                            j_update.status_castor_mcgen = 'ready'
                        
                    try:
                        j_update.save()
                        logger.info('Job %s with PandaID %s updated' % (j_update.id, j_update.panda_id)) 
                    except IntegrityError as e:
                        logger.exception('Unique together catched, was not saved')
                    except DatabaseError as e:
                        logger.exception('Something went wrong while saving: %s' % e.message)
                
    logger.info('done')

if __name__ == "__main__":
    sys.exit(main())
