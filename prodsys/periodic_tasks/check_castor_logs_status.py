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
from fabric.api import env, run, execute, settings as sett, hide
from fabric.context_managers import shell_env, cd
import csv

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.check_castor_logs_status.log')

today = datetime.datetime.today()
logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process('check_castor_logs_status.py', pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

env.hosts = []
env.hosts.append(settings.COMPASS_HOST)
env.user = settings.COMPASS_USER
env.password = settings.COMPASS_PASS

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def check_files_on_castor():
    logger.info('Getting productions castor status archiving')
    tasks_list = list(Task.objects.all().exclude(site='BW_COMPASS_MCORE').filter(status='archiving').values_list('production', 'path', 'soft').distinct())
    logger.info('Got list of %s productions' % len(tasks_list))
    
    for t in tasks_list:
        logger.info('Going to check file migration status on Castor')
        file = '%s_logFiles.tarz' % t[2]
        file_and_path = '/castor/cern.ch/user/n/na58dst1/prodlogs/testproductions/' + file
        cmd = 'nsls -l ' + file_and_path
        logger.info(cmd)
        result = exec_remote_cmd(cmd)
        if result.succeeded:
            reader = csv.DictReader(result.splitlines(), delimiter = ' ', skipinitialspace = True, fieldnames = ['permissions', 'links', 'owner', 'group', 'size', 'date1', 'date2', 'time', 'name'])
            logger.info('Successfully read file on Castor for production %s' % t[0])
            for r in reader:
                if r['name'].find(file) != -1:
                    logger.info(r)
                    logger.info('Found "%s" for production %s, %s' % (r['permissions'][0], t[0], file))
                    if r['permissions'][0] == 'm':
                        logger.info ('Going to update tasks of production %s as migrated' % t[0])
                        try:
                            task_update = Task.objects.filter(production=t[0]).update(status='archived', date_updated=today)
                            logger.info('Tasks status changed to archived for production %s' % t[0])
                        except:
                            logger.error('Failed to update tasks for production' % t[0])
                    else:
                        logger.info ('File not yet migrated')
                        
                        if r['size'] == '0':
                            logger.info('Problematic file found, status will be changed to archive for rewriting')
                            try:
                                task_update = Task.objects.filter(production=t[0]).update(status='archive', date_updated=today)
                                logger.info('Tasks status changed to archive for production %s' % t[0])
                            except:
                                logger.error('Failed to update tasks for production' % t[0])
                    
                    break
        else:
            logger.info('Error reading files on Castor for production %s' % t[0])
            logger.error(result)
    
        logger.info('done')
