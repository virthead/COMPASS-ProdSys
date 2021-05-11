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

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def delete_task_files():
    logger.info('Getting tasks with status=archived and status_files_deleted=no')
    tasks_list = Task.objects.all().exclude(Q(site='BW_COMPASS_MCORE') | Q(site='BW_STAMPEDE_MCORE') | Q(site='BW_FRONTERA_MCORE')).filter(status='archived').filter(status_files_deleted='no').order_by('id')
    logger.info('Got list of %s tasks' % len(tasks_list))
    i = 0
    for t in tasks_list:
        if i >= 2:
            logger.info('Reached deletion limit, exiting')
            break
        
        dirs = {}
        mc = ''
        if t.type == 'test production' or t.type == 'mass production' or t.type == 'technical production':
            dirs = {'evtdump': 0, 'mergedDump': 0, 'mDST.chunks': 0, 'TRAFDIC': 0, 'logFiles': 0}
        
        if t.type == 'DDD filtering':
            dirs = {'evtdump': 0, 'mergedDump': 0, 'logFiles': 0}
            
        if t.type == 'MC generation':
            dirs = {'xmls': 0, 'mcgen': 0, 'logFiles': 0}
            mc = 'mc/'
            
        if t.type == 'MC reconstruction':
            dirs = {'mDST.chunks': 0, 'TRAFDIC': 0, 'logFiles': 0}
            mc = 'mc/'
        
        logger.info('Task %s' % t.name)
        for dir, val in dirs.iteritems():
            logger.info('Going to delete directory %s' % dir)
            cmd = 'eos rm -rf %(eosCompassHome)s%(mc)s%(prodPath)s%(prodSoft)s/%(dir)s' % {'eosCompassHome': settings.EOS_HOME, 'mc': mc, 'prodPath': t.path, 'prodSoft': t.soft, 'dir': dir}
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1:
                logger.info('Session expired, exiting')
                break
           
            logger.info('Going to check if log files of %s exist' % dir)
            cmd = 'eos ls %(eosCompassHome)s%(mc)s%(prodPath)s%(prodSoft)s/%(dir)s' % {'eosCompassHome': settings.EOS_HOME, 'mc': mc, 'prodPath': t.path, 'prodSoft': t.soft, 'dir': dir}
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1:
                logger.info('Session expired, exiting')
                break
            if result.find('No such file or directory') != -1:
                logger.info('%s dir was deleted' % dir)
                dirs[dir] = 1
                continue
            
            logger.info('Going to delete all files from %s' % dir)
            
            slot = ''
            if dir == 'evtdump':
                slot = 'slot%(prodSlt)s/' % {'prodSlt': t.prodslt}
            
            files_removed = 0
            files_to_remove_limit = 1000
            files_arr = result.split('\n')
            
            logger.info('Got array of %s files' % len(files_arr))
            for file in files_arr:
                cmd = "eos rm %(eosCompassHome)s%(mc)s%(prodPath)s%(prodSoft)s/%(dir)s/%(slot)s%(file)s" % {'eosCompassHome': settings.EOS_HOME, 'mc': mc, 'prodPath': t.path, 'prodSoft': t.soft, 'dir': dir, 'slot': slot, 'file': file.rstrip()}
                logger.info(cmd)
                result = exec_remote_cmd(cmd)
                logger.info(result)
                if result.find('Permission denied') != -1:
                    logger.info('Session expired, exiting')
                    break
            
                files_removed += 1
        
                if files_removed >= files_to_remove_limit:
                    logger.info('Limit of %s files to be removed reached, exiting' % files_to_remove_limit)
                    break
            
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
