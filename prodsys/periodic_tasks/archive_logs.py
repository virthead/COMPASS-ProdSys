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
getRotatingFileHandler(logger, 'periodic_tasks.archive_logs.log')

logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
logger.info('__file__: %s' % __file__)

if check_process("archive_logs.py", pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

env.hosts = []
env.hosts.append(settings.COMPASS_HOST)
env.user = settings.COMPASS_USER
env.password = settings.COMPASS_PASS

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def archive_logs():
    logger.info('Getting tasks with status done, archive and archived')
    tasks_list = Task.objects.all().exclude(site='BW_COMPASS_MCORE').filter(status='archive')
    logger.info('Got list of %s tasks' % len(tasks_list))
    for t in tasks_list:
        logger.info('Going to archive log files of task %s' % t)
        
        cmd = 'tar -zcf /eos/experiment/compass/%(Path)s%(Soft)s/%(Soft)s_logFiles.tarz /eos/experiment/compass/%(Path)s%(Soft)s/logFiles' % {'Path': t.path, 'Soft': t.soft}
        logger.info(cmd)
        result = exec_remote_cmd(cmd)
        logger.info(result)
        if result.find('Permission denied') != -1:
            logger.info('Session expired, exiting')
            sys.exit(0)
        if result.succeeded:
            logger.info('Going to check if generated file exists')
            cmd1 = 'ls /eos/experiment/compass/%(Path)s%(Soft)s/%(Soft)s_logFiles.tarz' % {'Path': t.path, 'Soft': t.soft}
            logger.info(cmd1)
            result = exec_remote_cmd(cmd1)
            logger.info(result)
            if result.find('Permission denied') != -1:
                logger.info('Session expired, exiting')
                sys.exit(0)
                if result.succeeded:
                    logger.info('Going to copy archive to Castor')
                else:
                    logger.info('Error reading archive, skipping')
                    continue
        else:
            logger.info('Error generating archive, skipping')
            continue
        
        break
        
    logger.info('done')
