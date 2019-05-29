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
getRotatingFileHandler(logger, 'periodic_tasks.delete_task_files.log')

logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
logger.info('__file__: %s' % __file__)

if check_process("delete_task_files.py", pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

env.hosts = []
env.hosts.append(settings.COMPASS_HOST)
env.user = settings.COMPASS_USER
env.password = settings.COMPASS_PASS

dirs = {'evtdump': 0, 'mergedDump': 0, 'logFiles': 0, 'mDST.chunks': 0, 'TRAFDIC': 0}

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def delete_task_files():
    logger.info('Getting tasks with status=archived and status_files_deleted=no')
    tasks_list = Task.objects.all().exclude(site='BW_COMPASS_MCORE').filter(status='archived').filter(status_files_deleted='no').order_by('-id')
    logger.info('Got list of %s tasks' % len(tasks_list))
    i = 0
    for t in tasks_list:
        if i >= 1:
            logger.info('Reached deletion limit, exiting')
            break
        
        logger.info('Task %s' % t.name)
        for dir, val in dirs.iteritems():
            if t.type == 'DDD filtering':
                if dir == 'TRAFDIC' or dir == 'histos' or dir == 'mDST.chunks':
                    logger.info('DDD filtering task, skipping %s' % dir)
                    dirs[dir] = 1
                    continue
                
            logger.info('Going to delete directory %s' % dir)
            cmd = 'rm -rf %(eosCompassHome)s%(prodPath)s%(prodSoft)s/%(dir)s' % {'eosCompassHome': settings.EOS_HOME, 'prodPath': t.path, 'prodSoft': t.soft, 'dir': dir}
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1:
                logger.info('Session expired, exiting')
                break
           
            logger.info('Going to check if log files of %s exist' % dir)
            cmd = 'ls %(eosCompassHome)s%(prodPath)s%(prodSoft)s/%(dir)s' % {'eosCompassHome': settings.EOS_HOME, 'prodPath': t.path, 'prodSoft': t.soft, 'dir': dir}
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1:
                logger.info('Session expired, exiting')
                break
            if result.find('No such file or directory') != -1:
                logger.info('%s dir was deleted' % dir)
                dirs[dir] = 1
        
        i += 1
        
        problem_occured = False
        for dir, val in dirs.iteritems():
            if val != 1:
                problem_occured = True
                logger.info('Dir %s was not deleted' % dir)
        
        if problem_occured == False:
            logger.info('All dirs of task %s were deleted, going to update status_files_deleted' % t.name)
            t_update = Task.objects.filter(id=t.id).update(status_files_deleted='yes')
        
    logger.info('done')
