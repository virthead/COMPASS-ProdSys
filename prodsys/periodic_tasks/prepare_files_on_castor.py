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
from fabric.api import env, run, execute, settings as sett, hide
from fabric.context_managers import shell_env, cd
import csv

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job
from schedconfig.models import Jobsactive4

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.prepare_files_on_castor.log')

logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
logger.info('__file__: %s' % __file__)

if check_process("prepare_files_on_castor.py", pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

env.hosts = []
env.hosts.append(settings.COMPASS_HOST)
env.user = settings.COMPASS_USER
env.password = settings.COMPASS_PASS

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def prepare_on_castor():
    logger.info('Getting tasks with status send and running for BlueWaters')
    tasks_list = Task.objects.all().filter(site='BW_COMPASS_MCORE').filter(Q(status='send') | Q(status='running'))
    logger.info('Got list of %s tasks' % len(tasks_list))
    for t in tasks_list:
        logger.info('Going to update jobs for %s task on BlueWaters to staged' % t)
        jobs_list = Job.objects.filter(task=t).filter(status='defined').update(status='staged', date_updated=timezone.now())
        logger.info('Job status of task %s for BlueWaters was changed to staged' % t)
        
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
    
    logger.info('Getting tasks with status send and running for all sites except BlueWaters')
    tasks_list = Task.objects.all().exclude(site='BW_COMPASS_MCORE').filter(Q(status='send') | Q(status='running'))
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
            if i > 5:
                break
            
            if t.files_source == 'runs list':
                logger.info('In runs list branch')
                
                logger.info('Going to generate file with files list for run number %s' % run_number)
                cmd = '/eos/user/n/na58dst1/production/GetFileList.pl %s' % run_number
                logger.info(cmd)
                result = exec_remote_cmd(cmd)
                logger.info(result)
                if result.find('Permission denied') != -1:
                    logger.info('Session expired, exiting')
                    break
                
                if result.find(' found for run %s in the DB. (see file Run_%s.list)' % (run_number, run_number)) == -1:
                    logger.info('Error building list of files for run %s, skipping' % run_number)
                    logger.error(result)
                    continue
            else:
                logger.info('In files list branch')
                
                logger.info('Going to generate file with files list for run number %s' % run_number)
                single_job = Job.objects.filter(task=t).filter(run_number=run_number)[0]
                path = single_job.file[:single_job.file.rfind('/') + 1].replace('/', '\/')
                cmd = "nsls %s | grep %s | sed 's/^/%s/' > Run_%s.list" % (path, run_number, path, run_number)
                logger.info(cmd)
                result = exec_remote_cmd(cmd)
                logger.info(result)
                if result.find('Permission denied') != -1:
                    logger.info('Session expired, exiting')
                    break
            
            cmd1 = 'stager_get -f Run_%s.list -S compasscdr -U %s' % (run_number, run_number)
            logger.info(cmd1)
            result1 = exec_remote_cmd(cmd1)
            if result1.succeeded:
                logger.info('Successfully sent request to castor')
                logger.info(result1)
                
                logger.info('Going to update job statuses of run %s to staging' % run_number)
                jobs_list = Job.objects.filter(task=t).filter(run_number=run_number).filter(status='defined').update(status='staging')
                
                i += 1
            else:
                logger.info('Error sending request to castor')
                logger.error(result1)
            
        logger.info('Getting runs with job status staging for task %s' % t.name)
        runs_list = Job.objects.filter(task=t).filter(status='staging').order_by('run_number').values_list('run_number', flat=True).distinct()
        logger.info('Got list of %s runs' % len(runs_list))
        if len(runs_list) == 0:
            logger.info('No runs found for staging')
        
        for run_number in runs_list:
            logger.info('Going to request state of tag %s' % run_number)
            
            cmd1 = 'stager_qry -S compasscdr -U %s' % run_number
            logger.info(cmd1)
            result1 = exec_remote_cmd(cmd1)
            logger.info(result1)
            
            if result1.succeeded:
                logger.info('Successfully sent request to castor')
                logger.info(result1)
                
                logger.info('Going to update job statuses of run %s from staging to staged' % run_number)
                reader = csv.DictReader(result1.splitlines(), delimiter = ' ', skipinitialspace = True, fieldnames = ['file', 'owner', 'status'])
                
                jobs_list_update = Job.objects.filter(task=t).filter(run_number=run_number).filter(status='staging')
                for r in reader:
                    if r['status'] == 'STAGED':
                        logger.info('File %s has status STAGED, going to get a job' % r['file'])
                        for j in jobs_list_update:
                            if r['file'] == j.file:
                                j_update = Job.objects.get(file=r['file'], task__id=t.id)
                                if j_update.status == 'staging':
                                    logger.info('Status of job %s is staging, going to update to staged' % r['file'])
                                    j_update.status = 'staged'
                                    j_update.date_updated = timezone.now()
                        
                                    try:
                                        j_update.save()
                                        logger.info('Job %s updated at %s' % (j_update.id, timezone.now())) 
                                    except IntegrityError as e:
                                        logger.exception('Unique together catched, was not saved')
                                    except DatabaseError as e:
                                        logger.exception('Something went wrong while saving: %s' % e.message)
                        
            else:
                logger.info('Error sending request to castor')
                logger.error(result1)
            
    logger.info('done')
