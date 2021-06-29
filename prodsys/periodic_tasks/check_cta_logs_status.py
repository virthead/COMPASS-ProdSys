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
getRotatingFileHandler(logger, 'periodic_tasks.check_cta_logs_status.log')

logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process('check_cta_logs_status.py', pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

env.hosts = []
env.hosts.append(settings.COMPASS_HOST)
env.user = settings.COMPASS_USER
env.password = settings.COMPASS_PASS

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def check_files_on_cta():
    logger.info('Getting productions cta status archiving')
    tasks_list = list(Task.objects.all().exclude(Q(site='BW_COMPASS_MCORE') | Q(site='BW_STAMPEDE_MCORE') | Q(site='BW_FRONTERA_MCORE')).filter(status='archiving').values_list('production', 'path', 'soft', 'type', 'year').distinct())
    logger.info('Got list of %s productions' % len(tasks_list))
    
    for t in tasks_list:
        logger.info('Going to check file migration status on cta')
        path = settings.CTA_HOME_LOGS
        if t[3] == 'mass production':
            path = path + '%s/' % t[4] 
            file = '%s_logFiles.tarz' % t[0]
            file_and_path = path + file
        elif t[3] == 'MC generation':
            file = '%s_logFiles.tarz' % t[2]
            file_and_path = path + 'mc_prod/gen/' + file
        elif t[3] == 'MC reconstruction':
            file = '%s_logFiles.tarz' % t[2]
            file_and_path = path + 'mc_prod/reco/' + file
        else:
            file = '%s_logFiles.tarz' % t[2]
            file_and_path = path + 'testproductions/' + file
        
        cmd = 'xrdfs %(ctaHomeRoot)s ls -l %(path)s' % {'ctaHomeRoot': settings.CTA_HOME_ROOT, 'path': file_and_path}
        logger.info(cmd)
        result = exec_remote_cmd(cmd)
        if result.succeeded:
            reader = csv.DictReader(result.splitlines(), delimiter = ' ', skipinitialspace = True, fieldnames = ['permissions', 'date', 'time', 'size', 'name'])
            logger.info('Successfully read file on cta for production %s' % t[0])
            for r in reader:
                if r['name'].find(file) != -1:
                    logger.info(r)
                    logger.info('Found "%s" for production %s, %s' % (r['permissions'][0], t[0], file))
                    
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
                                jobs_update = Job.objects.filter(task__production=t[0]).update(status_logs_castor='yes', date_updated=timezone.now())
                                task_update = Task.objects.filter(production=t[0]).update(status='archived', date_processing_finish=timezone.now(), date_updated=timezone.now())
                                logger.info('Tasks status changed to archived for production %s' % t[0])
                            except:
                                logger.error('Failed to update tasks for production' % t[0])
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
                            try:
                                task_update = Task.objects.filter(production=t[0]).update(status='archive', date_updated=timezone.now())
                                logger.info('Tasks status changed to archive for production %s' % t[0])
                            except:
                                logger.error('Failed to update tasks for production' % t[0])
                    
                    break
        else:
            logger.info('Error reading files on cta for production %s' % t[0])
            logger.error(result)
            
            if result.find('No such file or directory') != -1:
                logger.info('Problematic file found, status will be changed to archive for rewriting')
                try:
                    task_update = Task.objects.filter(production=t[0]).update(status='archive', date_updated=timezone.now())
                    logger.info('Tasks status changed to archive for production %s' % t[0])
                except:
                    logger.error('Failed to update tasks for production' % t[0])
    
        logger.info('done')
