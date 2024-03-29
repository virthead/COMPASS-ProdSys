#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import commands
import datetime
from django.utils import timezone
from django.conf import settings
import logging
from django.core.wsgi import get_wsgi_application
from django.db import DatabaseError, IntegrityError
from _mysql import NULL
from fabric.api import env, run, execute, settings as sett, hide
from fabric.context_managers import shell_env, cd
import csv
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.check_cta_mcgen_status.log')

logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process('check_cta_mcgen_status.py', pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

env.hosts = []
env.hosts.append(settings.COMPASS_HOST)
env.user = settings.COMPASS_USER
env.password = settings.COMPASS_PASS

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def restart_transfer(logger, task, run_number, chunk_number):
    try:
        j_update = Job.objects.filter(task=task, run_number=run_number, chunk_number=chunk_number).update(status_castor_mcgen='ready', date_updated=timezone.now())
        logger.info('Job status_castor_mcgen changed to ready for task %s run number %s chunk number %s' % (task, run_number, chunk_number))
    except:
        logger.error('Failed to update jobs for task %s run number %s chunk number %s' % (task, run_number, chunk_number))
    
    return True

def check_files_on_cta():
    logger.info('Getting productions with castor mcgen status sent')
    tasks_list = Job.objects.filter(status_castor_mcgen='sent').values_list('task_id', 'task__year', 'task__period', 'task__soft').distinct()
    logger.info('Got list of %s prods: %s' % (len(tasks_list), tasks_list))
    logger.info('Check details in the corresponding periodic_tasks.check_cta_mcgen_status_taskid.log')
    
    for t in tasks_list:
        logger_task = logging.getLogger('periodic_tasks_logger')
        getRotatingFileHandler(logger_task, 'periodic_tasks.check_cta_mcgen_status_%s.log' % t[0])
        logger_task.info('Starting')
        
        logger_task.info('Getting mcgen chunks with castor mcgen status sent')
        chunks_list = Job.objects.filter(task__id=t[0]).filter(status_castor_mcgen='sent').values_list('task_id', 'run_number', 'chunk_number', 'date_updated').distinct()
        logger_task.info('Got list of %s chunks' % len(chunks_list))
    
        logger_task.info('Going to request list of files on cta for task %s' % t[0])
        
        path = '%(ctaHome)smc_prod/CERN/%(Year)s/%(Period)s/%(prodSoft)s/mcgen/' % {'ctaHome': settings.CTA_HOME, 'Year': t[1], 'Period': t[2], 'prodSoft': t[3]}
        
        cmd = 'xrdfs %(ctaHomeRoot)s ls -l %(path)s' % {'ctaHomeRoot': settings.CTA_HOME_ROOT, 'path': path}
        logger_task.info(cmd)
        result = exec_remote_cmd(cmd)
        if result.succeeded:
            logger_task.info('Successfully read files on cta for task %s' % t[0])
            for c in chunks_list:
                reader = csv.DictReader(result.splitlines(), delimiter = ' ', skipinitialspace = True, fieldnames = ['permissions', 'date', 'time', 'size', 'name'])
                
                found = False
                test = '%(path)smcr%(chunkNumber)s-%(runNumber)s_run000.tgeant' % {'path': path, 'chunkNumber': format(c[2], '05d'), 'runNumber': c[1]}
                
                for r in reader:
                    if r['name'] == test:
                        found = True
                        logger_task.info(r)
                        logger_task.info('Found "%s" for task id %s run number %s chunk number %s, %s' % (r['permissions'][0], t[0], c[1], c[2], test))
                        
                        cmd1 = 'xrdfs %s query prepare %s %s' % (settings.CTA_HOME_ROOT, r['name'], r['name'])
                        logger.info(cmd1)
                        result1 = exec_remote_cmd(cmd1)
                        if result1.succeeded:
                            logger.info('Successfully sent request to cta')
                            logger.info(result1)
                            
                            data = json.loads(result1)
                            responses = data['responses']
                            
                            if responses[0]['on_tape']:
                                logger_task.info ('Going to update jobs of the chunk as migrated')
                                try:
                                    j_update = Job.objects.filter(task=t[0], run_number=c[1], chunk_number=c[2]).update(status_castor_mcgen='finished', date_updated=timezone.now())
                                    logger_task.info('Job status_castor_mcgen changed to finished for task %s run number %s chunk number %s' % (t[0], c[1], c[2]))
                                except:
                                    logger_task.error('Failed to update jobs for task %s run number %s chunk number %s' % (t[0], c[1], c[2]))
                            else:
                                logger_task.info('Chunk not yet migrated')
                        else:
                            logger.info('Error sending request to cta')
                            logger.error(result1)
                        
                        if r['size'] == '0':
                            diff = datetime.datetime.now().replace(tzinfo=None) - c[3].replace(tzinfo=None)
                            logger_task.info('File %s was not delivered, transfer was submitted at %s which is %s hours from now' % (test, c[3], (diff.seconds/3600)))
                            if diff.seconds/3600 >= 12:
                                logger_task.info('Problematic chunk found, status will be changed to ready for rewriting')
                                restart_transfer(logger_task, t[0], c[1], c[2])
                        
                        break
                
                if found is False:
                    diff = datetime.datetime.now().replace(tzinfo=None) - c[3].replace(tzinfo=None)
                    logger_task.info('File %s was not delivered, transfer was submitted at %s which is %s hours from now' % (test, c[3], (diff.seconds/3600)))
                    if diff.seconds/3600 >= 1:
                        logger.info('Transfer request was performed in more than 1 hours ago, going to restart it')
                        restart_transfer(logger_task, t[0], c[1], c[2])
        else:
            logger_task.info('Error reading files on cta for task %s' % t[0])
            logger_task.error(result)
    
        logger_task.info('done')
        logger_task.handlers[0].close()
