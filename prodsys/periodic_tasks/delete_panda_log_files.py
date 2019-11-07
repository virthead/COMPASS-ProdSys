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
getRotatingFileHandler(logger, 'periodic_tasks.delete_panda_log_files.log')

logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
logger.info('__file__: %s' % __file__)

if check_process("delete_panda_log_files.py", pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

env.hosts = []
env.hosts.append(settings.COMPASS_HOST)
env.user = settings.COMPASS_USER
env.password = settings.COMPASS_PASS

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def delete_panda_log_files():
    logger.info('Getting tasks with status archive')
    tasks_list = Task.objects.all().exclude(Q(site='BW_COMPASS_MCORE') | Q(site='BW_STAMPEDE_MCORE') | Q(site='BW_FRONTERA_MCORE')).filter(status='archive').order_by('-id')
    logger.info('Got list of %s tasks' % len(tasks_list))
    for t in tasks_list:
        logger.info('Getting runs for task %s' % t.name)
        runs_list = Job.objects.filter(task=t).filter(status_logs_deleted='no').order_by('run_number').values_list('run_number', flat=True).distinct()
        logger.info('Got list of %s runs' % len(runs_list))
        if len(runs_list) == 0:
            logger.info('No runs found')
        
        prod = False
        mdst = False
        hist = False
        dump = False
        
        all = False
        
        i = 0
        for run_number in runs_list:
            if i > 5:
                break
            
            pars = {'eosHome': settings.EOS_HOME, 'eosHomeLogs': settings.EOS_HOME_LOGS, 'Path': t.path, 'Production': t.production, 'runNumber': run_number}
            
            if t.type == 'test production' or t.type == 'mass production' or t.type == 'technical production' or t.type == 'DDD filtering' or t.type == 'MC generation':
                logger.info('Going to delete log files of prod job for run number %s' % run_number)
                cmd = 'rm %(eosHome)s%(eosHomeLogs)s%(Production)s-%(runNumber)s-*.job.log.tgz' % pars
                logger.info(cmd)
                result = exec_remote_cmd(cmd)
                logger.info(result)
                if result.find('Permission denied') != -1:
                    logger.info('Session expired, exiting')
                    break
            
                logger.info('Going to check if log files of prod job for run number %s exist' % run_number)
                cmd = 'ls %(eosHome)s%(eosHomeLogs)s%(Production)s-%(runNumber)s-*.job.log.tgz' % pars
                logger.info(cmd)
                result = exec_remote_cmd(cmd)
                logger.info(result)
                if result.find('Permission denied') != -1:
                    logger.info('Session expired, exiting')
                    break
                if result.find('No such file or directory') != -1:
                    logger.info('prod log file for run %s deleted' % run_number)
                    prod = True
                
            if t.type == 'test production' or t.type == 'mass production' or t.type == 'technical production' or t.type == 'DDD filtering':
                logger.info('Going to delete log files of dump merging job for run number %s' % run_number)
                cmd = 'rm %(eosHome)s%(eosHomeLogs)s%(Production)s-merge-dump-%(runNumber)s-*.job.log.tgz' % pars
                logger.info(cmd)
                result = exec_remote_cmd(cmd)
                logger.info(result)
                if result.find('Permission denied') != -1:
                    logger.info('Session expired, exiting')
                    break
                
                logger.info('Going to check if log files of dump merging job for run number %s exist' % run_number)
                cmd = 'ls %(eosHome)s%(eosHomeLogs)s%(Production)s-merge-dump-%(runNumber)s-*.job.log.tgz' % pars
                logger.info(cmd)
                result = exec_remote_cmd(cmd)
                logger.info(result)
                if result.find('Permission denied') != -1:
                    logger.info('Session expired, exiting')
                    break
                if result.find('No such file or directory') != -1:
                    logger.info('dump merging log file for run %s deleted' % run_number)
                    dump = True
                
            if t.type == 'test production' or t.type == 'mass production' or t.type == 'technical production':
                logger.info('Going to delete log files of mdst merging job for run number %s' % run_number)
                cmd = 'rm %(eosHome)s%(eosHomeLogs)s%(Production)s-merge-%(runNumber)s-*.job.log.tgz' % pars
                logger.info(cmd)
                result = exec_remote_cmd(cmd)
                logger.info(result)
                if result.find('Permission denied') != -1:
                    logger.info('Session expired, exiting')
                    break
                
                logger.info('Going to check if log files of mdst merging job for run number %s exist' % run_number)
                cmd = 'ls %(eosHome)s%(eosHomeLogs)s%(Production)s-merge-%(runNumber)s-*.job.log.tgz' % pars
                logger.info(cmd)
                result = exec_remote_cmd(cmd)
                logger.info(result)
                if result.find('Permission denied') != -1:
                    logger.info('Session expired, exiting')
                    break
                if result.find('No such file or directory') != -1:
                    logger.info('mdst merging log file for run %s deleted' % run_number)
                    mdst = True
        
                logger.info('Going to delete log files of hist merging job for run number %s' % run_number)
                cmd = 'rm %(eosHome)s%(eosHomeLogs)s%(Production)s-merge-hist-%(runNumber)s-*.job.log.tgz' % pars
                logger.info(cmd)
                result = exec_remote_cmd(cmd)
                logger.info(result)
                if result.find('Permission denied') != -1:
                    logger.info('Session expired, exiting')
                    break
                
                logger.info('Going to check if log files of hist merging job for run number %s exist' % run_number)
                cmd = 'ls %(eosHome)s%(eosHomeLogs)s%(Production)s-merge-hist-%(runNumber)s-*.job.log.tgz' % pars
                logger.info(cmd)
                result = exec_remote_cmd(cmd)
                logger.info(result)
                if result.find('Permission denied') != -1:
                    logger.info('Session expired, exiting')
                    break
                if result.find('No such file or directory') != -1:
                    logger.info('hist merging log file for run %s deleted' % run_number)
                    hist = True
            
            if t.type == 'test production' or t.type == 'mass production' or t.type == 'technical production':
                if prod and dump and mdst and hist:
                    all = True
            
            if t.type == 'DDD filtering':
                if prod and dump:
                    all = True
            
            if t.type == 'MC generation':
                if prod:
                    all = True
            
            if all:        
                logger.info('All types of log files were deleted, going to update jobs for run %s' % run_number)
                u = Job.objects.filter(task=t).filter(run_number=run_number).update(status_logs_deleted='yes', date_updated=timezone.now())
                
            i+= 1
    logger.info('done')
