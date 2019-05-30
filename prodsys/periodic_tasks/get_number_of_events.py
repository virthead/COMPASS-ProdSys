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
from posix import access

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job

from utils import check_process, getRotatingFileHandler

max_check_amount = 3000

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.get_number_of_events.log')

today = datetime.datetime.today()
logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
logger.info('__file__: %s' % __file__)

if check_process("get_number_of_events.py", pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

env.hosts = []
env.hosts.append(settings.COMPASS_HOST)
env.user = settings.COMPASS_USER
env.password = settings.COMPASS_PASS

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def get_number_of_events():
    logger.info('Getting tasks with status jobs ready or send of running')
    tasks_list = Task.objects.all().exclude(type='MC generation').filter(Q(status='jobs ready') | Q(status='send') | Q(status='running')).order_by('-id')
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    access_denied = False
    for t in tasks_list:
        if access_denied:
            break
        
        logger.info('Getting jobs with number_of_events=-1 for task %s' % t.name)
        jobs_list = Job.objects.filter(task=t).filter(number_of_events_attempt__lt=1).filter(number_of_events=-1).order_by('run_number')
        logger.info('Got list of %s jobs' % len(jobs_list))
        
        i = 0
        for j in jobs_list:
            if i > max_check_amount:
                break
            
            number_of_events = 0
            result = ''
            try:
                cmd = '/eos/user/n/na58dst1/production/GetEventNumber.pl %s' % j.file[j.file.rfind('/') + 1:]
                logger.info(cmd)
                result = exec_remote_cmd(cmd)
                logger.info(result)
                
                if result.find('Permission denied') != -1 or result.find('ORA-12514: TNS:listener does not currently know of service requested in connect') != -1:
                    access_denied = True
                    logger.info('Session expired, exiting')
                    break
            except:
                logger.error('Failed to extract file name from %s' % j.file)
                continue
            
            if result.find('Number of events:') != -1:
                logger.info('Number of events info was generated')
                try:
                    number_of_events = [int(s) for s in result.split() if s.isdigit()][0]
                    logger.info('Got number of events %s' % number_of_events)
                    logger.info('Going to update job %s' % j.file)
                
                    j_update = Job.objects.get(id=j.id)
                    j_update.number_of_events=number_of_events
                    try:
                        j_update.save()
                        logger.info('Job %s updated' % (j_update.id)) 
                    except IntegrityError as e:
                        logger.exception('Unique together catched, was not saved')
                    except DatabaseError as e:
                        logger.exception('Something went wrong while saving: %s' % e.message)
                except:
                    logger.error('Noninteger result, going to update attempt number and skip')
                    j_update = Job.objects.get(id=j.id)
                    j_update.number_of_events_attempt=j_update.number_of_events_attempt + 1
                    try:
                        j_update.save()
                        logger.info('Job %s updated' % (j_update.id)) 
                    except IntegrityError as e:
                        logger.exception('Unique together catched, was not saved')
                    except DatabaseError as e:
                        logger.exception('Something went wrong while saving: %s' % e.message)
                    continue
            
            i += 1
                
    logger.info('done')
