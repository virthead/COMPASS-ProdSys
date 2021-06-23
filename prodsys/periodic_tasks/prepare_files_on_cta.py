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
from fabric.api import env, run, execute, settings as sett, hide, put
from fabric.context_managers import shell_env, cd
import time
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job
from schedconfig.models import Jobsactive4

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.prepare_files_on_cta.log')

logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
logger.info('__file__: %s' % __file__)

if check_process("prepare_files_on_cta.py", pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

env.hosts = []
env.hosts.append(settings.COMPASS_HOST)
env.user = settings.COMPASS_USER
env.password = settings.COMPASS_PASS

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def prepare_on_cta():
    with cd('/tmp'):
        years_in_text_files = [2015, 2016, 2017, 2018]
        
        logger.info('Getting tasks with status send and running for HPC')
        tasks_list = Task.objects.all().filter(Q(site='BW_COMPASS_MCORE') | Q(site='STAMPEDE_COMPASS_MCORE') | Q(site='FRONTERA_COMPASS_MCORE')).filter(Q(status='send') | Q(status='running'))
        logger.info('Got list of %s tasks' % len(tasks_list))
        for t in tasks_list:
            logger.info('Going to update jobs for %s task on HPC to staged' % t)
            jobs_list = Job.objects.filter(task=t).filter(status='defined').update(status='staged', date_updated=timezone.now())
            logger.info('Job status of task %s for HPC was changed to staged' % t)
            
            if not t.date_processing_start:
                logger.info('Going to update date_processing_start of %s task' % t)
                t_update = Task.objects.get(id=t.id)
                t_update.date_processing_start=timezone.now()
                t_update.date_updated = timezone.now()
                try:
                    t_update.save()
                    logger.info('Task %s updated' % t_update.name) 
                except IntegrityError as e:
                    logger.exception('Unique together catched, was not saved')
                except DatabaseError as e:
                    logger.exception('Something went wrong while saving: %s' % e.message)
        
        logger.info('Getting MC generation tasks with status send and running')
        tasks_list = Task.objects.all().filter(type='MC generation').filter(Q(status='send') | Q(status='running'))
        logger.info('Got list of %s tasks' % len(tasks_list))
        for t in tasks_list:
            logger.info('Going to update jobs for %s task to staged' % t)
            jobs_list = Job.objects.filter(task=t).filter(status='defined').update(status='staged', date_updated=timezone.now())
            logger.info('Job status of task %s was changed to staged' % t)
            
            if not t.date_processing_start:
                logger.info('Going to update date_processing_start of %s task' % t)
                t_update = Task.objects.get(id=t.id)
                t_update.date_processing_start=timezone.now()
                t_update.date_updated = timezone.now()
                try:
                    t_update.save()
                    logger.info('Task %s updated' % t_update.name) 
                except IntegrityError as e:
                    logger.exception('Unique together catched, was not saved')
                except DatabaseError as e:
                    logger.exception('Something went wrong while saving: %s' % e.message)
        
        logger.info('Getting count of staged files')
        jobs_list_count = Job.objects.filter(task__site='CERN_COMPASS_PROD').filter(Q(task__status='send') | Q(task__status='running')).filter(status='staged').count()
        logger.info('There are %s staged files' % jobs_list_count)
        if jobs_list_count > settings.MAX_STAGED_JOBS:
            logger.info('Maximum allowed staged reached, exiting')
            sys.exit(0)
        
        logger.info('Getting tasks with status send and running for all sites except HPC')
        tasks_list = Task.objects.all().exclude(Q(site='BW_COMPASS_MCORE') | Q(site='STAMPEDE_COMPASS_MCORE') | Q(site='FRONTERA_COMPASS_MCORE')).filter(tapes_backend='cta').filter(Q(status='send') | Q(status='running'))
        logger.info('Got list of %s tasks' % len(tasks_list))
        for t in tasks_list:
            if not t.date_processing_start:
                logger.info('Going to update date_processing_start of %s task' % t)
                t_update = Task.objects.get(id=t.id)
                t_update.date_processing_start=timezone.now()
                t_update.date_updated = timezone.now()
                try:
                    t_update.save()
                    logger.info('Task %s updated' % t_update.name) 
                except IntegrityError as e:
                    logger.exception('Unique together catched, was not saved')
                except DatabaseError as e:
                    logger.exception('Something went wrong while saving: %s' % e.message)
            
            logger.info('Getting runs with job status defined for task %s' % t.name)
            runs_list = Job.objects.filter(task=t).filter(status='defined').order_by('run_number').values_list('run_number', flat=True).distinct()
            logger.info('Got list of %s runs' % len(runs_list))
            if len(runs_list) == 0:
                logger.info('No runs found for staging')
            
            i = 0
            for run_number in runs_list:
                if i >= 5:
                    break
                
                # cmd = "nsls %s | grep %s | sed 's/^/%s/' > Run_%s.list" % (path, run_number, path, run_number)
                logger.info('Getting jobs from run %s with status defined' % run_number)
                jobs_list = Job.objects.filter(task=t).filter(run_number=run_number).filter(status='defined').values_list('file', flat=True).distinct()
                logger.info('Got list of %s jobs' % len(jobs_list))
                
                jobs_list_request = ' '
                for jj in jobs_list:
                    jobs_list_request += '%s ' % jj
                
                cmd1 = 'xrdfs %s prepare -s %s' % (settings.CTA_HOME_ROOT, jobs_list_request)
                logger.info(cmd1)
                result1 = exec_remote_cmd(cmd1)
                if result1.succeeded:
                    logger.info('Successfully sent request to cta')
                    logger.info(result1)
                    
                    logger.info('Going to update job statuses of run %s to staging' % run_number)
                    jobs_list = Job.objects.filter(task=t).filter(run_number=run_number).filter(status='defined').update(status='staging', cta_request_id=result1)
                    
                    i += 1
                else:
                    logger.info('Error sending request to cta')
                    logger.error(result1)
            
            logger.info('Getting runs with job status staging for task %s' % t.name)
            cta_requests_list = Job.objects.filter(task=t).filter(status='staging').order_by('cta_request_id').values_list('cta_request_id', flat=True).distinct()
            logger.info('Got list of %s runs' % len(runs_list))
            if len(cta_requests_list) == 0:
                logger.info('No cta requests found for staging')
            
            for cta_request_id in cta_requests_list:
                logger.info('Going to request state of request id %s' % cta_request_id)
                
                logger.info('Getting jobs from cta request %s with status staging' % cta_request_id)
                jobs_list_update = Job.objects.filter(task=t).filter(cta_request_id=cta_request_id).filter(status='staging').values_list('file', flat=True).distinct()
                logger.info('Got list of %s jobs' % len(jobs_list_update))
                
                jobs_list_request = ' '
                for jj in jobs_list_update:
                    jobs_list_request += '%s ' % jj
                
                cmd1 = 'xrdfs %s query prepare %s %s' % (settings.CTA_HOME_ROOT, cta_request_id, jobs_list_request)
                logger.info(cmd1)
                result1 = exec_remote_cmd(cmd1)
                logger.info(result1)
                
                if result1.succeeded:
                    logger.info('Successfully sent request to cta')
                    logger.info(result1)
                    
                    logger.info('Going to update job statuses of cta request %s from staging to staged' % cta_request_id)
                    data = json.loads(result1)
                    responses = data['responses']
                    
                    #jobs_list_update = Job.objects.filter(task=t).filter(run_number=run_number).filter(status='staging')
                    for r in responses:
                        if len(r['error_text']) > 0:
                            logger.info('Error message: %s' % r['error_text'])
                            logger.info('Going to reset status of %s to defined' % r['path'])
                            
                            j_update = Job.objects.get(file=r['path'], task__id=t.id)
                            j_update.status = 'defined'
                            j_update.date_updated = timezone.now()
                            
                            try:
                                j_update.save()
                                logger.info('Job %s updated at %s' % (j_update.id, timezone.now())) 
                            except IntegrityError as e:
                                logger.exception('Unique together catched, was not saved')
                            except DatabaseError as e:
                                logger.exception('Something went wrong while saving: %s' % e.message)
                            
                            continue
                        
                        if r['online']:
                            #logger.info('File %s has status %s, going to get a job' % (r['file'], r['status']))
                            for j in jobs_list_update:
                                if r['path'] == j:
                                    j_update = Job.objects.get(file=r['path'], task__id=t.id)
                                    if j_update.status == 'staging':
                                        logger.info('Status of job %s is staging, going to update to staged' % r['path'])
                                        j_update.status = 'staged'
                                        j_update.date_updated = timezone.now()
                            
                                        try:
                                            j_update.save()
                                            logger.info('Job %s updated at %s' % (j_update.id, timezone.now())) 
                                        except IntegrityError as e:
                                            logger.exception('Unique together catched, was not saved')
                                        except DatabaseError as e:
                                            logger.exception('Something went wrong while saving: %s' % e.message)
                                    
                                    continue
                            
                else:
                    logger.info('Error sending request to cta')
                    logger.error(result1)
            
                    #if result1.find('Unknown user tag') != -1:
                    #    logger.info('Tag %s has expired, going to reset it' % run_number)
                    #    jobs_list = Job.objects.filter(task=t).filter(run_number=run_number).filter(status='staging').update(status='defined')
                    
    logger.info('done')
