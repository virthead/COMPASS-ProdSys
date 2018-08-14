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
    tasks_list = Task.objects.all().filter(Q(status='jobs ready') | Q(status='send') | Q(status='running'))
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    for t in tasks_list:
        logger.info('Getting jobs with number_of_events=0 for task %s' % t.name)
        jobs_list = Job.objects.filter(task=t).filter(number_of_events=-1).order_by('run_number')
        logger.info('Got list of %s jobs' % len(jobs_list))
        
        i = 0
        for j in jobs_list:
            if i > max_check_amount:
                break
            
            number_of_events = 0
            
            logger.info('Going to check if file %s already in the system' % j.file)
            j_search = Job.objects.filter(run_number=j.run_number).filter(chunk_number=j.chunk_number).filter(number_of_events__gt=-1)
            if len(j_search) > 0:
                logger.info('File %s found in the system, going to inherit number of events' % j.file)
                j_update = Job.objects.get(id=j.id)
                j_update.number_of_events = j_search[0].number_of_events
                try:
                    j_update.save()
                    logger.info('Job %s with file %s updated at %s' % (j.id, j.file, today)) 
                except IntegrityError as e:
                    logger.exception('Unique together catched, was not saved')
                except DatabaseError as e:
                    logger.exception('Something went wrong while saving: %s' % e.message)
            
            else:
                logger.info('File %s was not found in the system, going to get number of events from the catalog' % j.file)
                
                result = ''
                try:
                    cmd = '/eos/user/n/na58dst1/production/GetEventNumber.pl %s' % j.file[j.file.rfind('/') + 1:]
                    logger.info(cmd)
                    result = exec_remote_cmd(cmd)
                    logger.info(result)
                except:
                    logger.error('Failed to extract file name from %s' % j.file)
                    if result.find('Permission denied') != -1:
                        logger.info('Session expired, exiting')
                        sys.exit()
                    continue
                
                if result.find('Number of events:') != -1:
                    logger.info('Number of events info was generated')
                    try:
                        number_of_events = [int(s) for s in result.split() if s.isdigit()][0]
                        logger.info('Got number of events %s' % number_of_events)
                        logger.info('Going to update job %s' % j.file)
                    
                        j_update = Job.objects.filter(file=j.file).update(number_of_events=number_of_events)
                    except:
                        logger.error('Noninteger result, skipping')
                        continue
            
            i += 1
                
    logger.info('done')
