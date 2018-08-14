#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import commands
from django.utils import timezone
import pytz
from django.conf import settings
import logging
from django.core.wsgi import get_wsgi_application
import subprocess
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.check_job_panda_status.log')

today = timezone.now()
logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process(__file__, pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

def get_running_processes():
    running_processes = []
    
    logger.info('Going to check running processes')
    output = subprocess.Popen("ps -eo args | grep 'python check_job_panda_status_run.py -t '", stdout=subprocess.PIPE, shell=True).communicate()
    if len(output[0]) != 0:
        logger.info('Found running processes')
        result = output[0].splitlines()
        for p in result:
            if p.find('grep') == -1:
                running_processes.append(p)
    
    logger.info('Found %s running processes' % len(running_processes))
    return running_processes

def main():
    logger.info('Getting tasks with status send, running and paused')
    tasks_list = Task.objects.all().filter(Q(status='send') | Q(status='running') | Q(status='paused')).order_by('-id')
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    for t in tasks_list:
        logger.info('Getting runs with jobs with status send and running for task %s' % t.name)
        runs_list = Job.objects.filter(task=t).filter(Q(status='sent') | Q(status='running')).order_by('run_number').values_list('run_number').distinct()
        logger.info('Got list of %s runs' % len(runs_list))
        if len(runs_list) == 0:
            continue
        
        logger.info('Starting fork cycle')
        for r in runs_list:
            running_processes = get_running_processes()
            
            if len(running_processes) > 15 and len(running_processes) <= 20:
                logger.info('Reached maximum of running processes, sleep 15 seconds')
                time.sleep(15)
            if len(running_processes) > 20:
                logger.info('Reached maximum of running processes, sleep 30 seconds')
                time.sleep(30)
            if len(running_processes) > 25:
                logger.info('Reached maximum of running processes, sleep 60 seconds')
                time.sleep(60)
            
            command = 'python check_job_panda_status_run.py -t %s -r %s' % (t.id, r[0])
            logger.info('Going to run command %s' % command)
            if len(running_processes) > 0:
                for p in running_processes:
                    if p.find(command) != -1:
                        logger.info('Command already running, skipping')
                        break
                     
                    x = subprocess.Popen(command, shell=True)
                    break
            else:
                x = subprocess.Popen(command, shell=True)
                
                            
    logger.info('done')

if __name__ == "__main__":
    sys.exit(main())
