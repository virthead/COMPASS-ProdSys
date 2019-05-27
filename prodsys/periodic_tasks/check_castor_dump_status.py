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
from prodsys.models import Task, Job

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.check_castor_dump_status.log')

today = datetime.datetime.today()
logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process('check_castor_status_dump.py', pid):
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
        j_update = Job.objects.filter(task=task, run_number=run_number, chunk_number_merging_evntdmp=chunk_number).update(status_castor_evntdmp='ready', date_updated=today)
        logger.info('Job status_castor_evntdmp changed to ready for task %s run number %s chunk number %s' % (task, run_number, chunk_number))
    except:
        logger.error('Failed to update jobs for task %s run number %s chunk number %s' % (task, run_number, chunk_number))
    
    return True

def check_files_on_castor():
    logger.info('Getting productions with castor evntdmp status sent')
    tasks_list = Job.objects.filter(status_castor_evntdmp='sent').values_list('task_id', 'task__path', 'task__soft', 'task__prodslt', 'task__phastver', 'task__type').distinct()
    logger.info('Got list of %s prods: %s' % (len(tasks_list), tasks_list))
    logger.info('Check details in the corresponding periodic_tasks.check_castor_dump_status_taskid.log')
    
    for t in tasks_list:
        logger_task = logging.getLogger('periodic_tasks_logger')
        getRotatingFileHandler(logger_task, 'periodic_tasks.check_castor_dump_status_%s.log' % t[0])
        logger_task.info('Starting')
        
        logger_task.info('Getting evntdmp chunks with castor evntdmp status sent')
        chunks_list = Job.objects.filter(task__id=t[0]).filter(status_castor_evntdmp='sent').values_list('task_id', 'run_number', 'chunk_number_merging_evntdmp', 'date_updated').distinct()
        logger_task.info('Got list of %s chunks' % len(chunks_list))
    
        logger_task.info('Going to request list of files on castor for task %s' % t[0])
        
        oracle_dst = ''
        if t[5] == 'mass production':
            oracle_dst = '/oracle_dst/'
        cmd = 'nsls -l /castor/cern.ch/compass/%(prodPath)s%(oracleDst)s%(prodSoft)s/mergedDump/slot%(prodSlt)s/' % {'prodPath': t[1], 'prodSoft': t[2], 'prodSlt': t[3], 'oracleDst': oracle_dst}
        logger_task.info(cmd)
        result = exec_remote_cmd(cmd)
        if result.succeeded:
            reader = csv.DictReader(result.splitlines(), delimiter = ' ', skipinitialspace = True, fieldnames = ['permissions', 'links', 'owner', 'group', 'size', 'date1', 'date2', 'time', 'name'])
            logger_task.info('Successfully read files on castor for task %s' % t[0])
            for c in chunks_list:
                found = False
                test = 'evtdump%(prodSlt)s-%(runNumber)s.raw' % {'runNumber': c[1], 'prodSlt': t[3]}
                if format(c[2], '03d') != '000':
                    test = test + '.' + format(c[2], '03d')
                
                for r in reader:
                    if r['name'] == test:
                        found = True
                        logger_task.info(r)
                        logger_task.info('Found "%s" for task id %s run number %s chunk number %s, %s' % (r['permissions'][0], t[0], c[1], c[2], test))
                        if r['permissions'][0] == 'm':
                            logger_task.info ('Going to update jobs of the chunk as migrated')
                            try:
                                j_update = Job.objects.filter(task=t[0], run_number=c[1], chunk_number_merging_evntdmp=c[2]).update(status_castor_evntdmp='finished', date_updated=today)
                                logger_task.info('Job status_castor_evntdmp changed to finished for task %s run number %s chunk number %s' % (t[0], c[1], c[2]))
                            except:
                                logger_task.error('Failed to update jobs for task %s run number %s chunk number %s' % (t[0], c[1], c[2]))
                        else:
                            logger_task.info ('Chunk not yet migrated')
                            
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
            logger_task.info('Error reading files on castor for task %s' % t[0])
            logger_task.error(result)
    
        logger_task.info('done')
        logger_task.handlers[0].close()
