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

runs_to_tar = 10

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
    logger.info('Getting tasks with status archive')
    tasks_list = list(Task.objects.all().exclude(site='BW_COMPASS_MCORE').filter(status='archive').values_list('production', 'path', 'soft').distinct()[:5])
    logger.info('Got list of %s productions' % len(tasks_list))
    access_denied = False
    for t in tasks_list:
        if access_denied:
            break
        
        logger.info('Getting runs for task %s' % t[0])
        runs_list = Job.objects.filter(task__production=t[0]).filter(status_logs_archived='no').values_list('run_number', flat=True).distinct()
        logger.info('Got list of %s runs' % len(runs_list))
        if len(runs_list) == 0:
            logger.info('No runs found')
        
        runs_tarred_prod = 0
        runs_tarred = 0
        for run_number in runs_list:
            if runs_tarred >= runs_to_tar:
                logger.info('Limit of runs to tar has reached, breaking')
                break
                
            logger.info('Going to check if tar for run %s exists on EOS' % run_number)
            cmd = 'ls /eos/experiment/compass/%(Path)s%(Soft)s/logFiles/%(Prod)s.%(run_number)s.tar' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2], 'run_number': run_number}
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1:
                logger.info('Session expired, exiting')
                break
            if result.find('No such file or directory') == -1:
                logger.info('Tar file for run %s exists, going to update chunks and continue' % run_number)
                jobs_update = Job.objects.filter(task__production=t[0]).filter(run_number=run_number).update(status_logs_archived='yes', date_updated=timezone.now())
                runs_tarred_prod += 1
                continue
            
            logger.info('Going to tar run %s' % run_number)
            cmd = 'tar -cvzf /tmp/%(Prod)s.%(run_number)s.tar /eos/experiment/compass/%(Path)s%(Soft)s/logFiles/%(Prod)s.%(run_number)s-*.gz' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2], 'run_number': run_number}
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1:
                logger.info('Session expired, exiting')
                access_denied = True
                break
            
            logger.info('Going to check if tar for run %s exists in /tmp' % run_number)
            cmd = 'ls /tmp/%(Prod)s.%(run_number)s.tar' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2], 'run_number': run_number}
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1:
                logger.info('Session expired, exiting')
                break
            if result.find('No such file or directory') == -1:
                logger.info('Tar file for run %s exists' % run_number)
            else:
                logger.info('Something went wrong, continue')
                continue
            
            logger.info('Going to move file from /tmp to EOS')
            cmd = 'mv /tmp/%(Prod)s.%(run_number)s.tar /eos/experiment/compass/%(Path)s%(Soft)s/logFiles/%(Prod)s.%(run_number)s.tar' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2], 'run_number': run_number}
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1:
                logger.info('Session expired, exiting')
                break
            
            runs_tarred_prod += 1
            runs_tarred += 1
        
        logger.info('%s of %s runs of production %s were archived and moved to EOS' % (runs_tarred_prod, len(runs_list), t[0])) 
        
        if runs_tarred_prod < len(runs_list):
            continue
        
        logger.info('All runs of %s are in tars, ready to create tar for production' % t[0])
        
        final_tarz_exists = False
        logger.info('Check if final tarz for %s exists on EOS' % t[0])
        cmd = 'ls /eos/experiment/compass/%(Path)s%(Soft)s/logFiles/%(Soft)s_logFiles.tarz' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2]}
        logger.info(cmd)
        result = exec_remote_cmd(cmd)
        logger.info(result)
        if result.find('Permission denied') != -1:
            logger.info('Session expired, exiting')
            break
        if result.find('No such file or directory') == -1:
            logger.info('Tar file for production %s exists, going to copy it to Castor' % t[0])
            final_tarz_exists = True
        else:
            logger.info('Tar file for production %s does not exist, going to generate it' % t[0])
        
        if not final_tarz_exists:
            logger.info('Going to create final tarz file for production %s' % t[0])
            cmd = 'tar -cvzf /tmp/%(Soft)s_logFiles.tarz /eos/experiment/compass/%(Path)s%(Soft)s/logFiles/%(Prod)s.*.tar' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2]}
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1:
                logger.info('Session expired, exiting')
                access_denied = True
                break
              
            if not result.succeeded:
                logger.info('Error generating archive, skipping')
                continue
            
            logger.info('Going to check if final tar for production %s exists in /tmp' % t[0])
            cmd = 'ls /tmp/%(Soft)s_logFiles.tarz' % {'Soft': t[2]}
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1:
                logger.info('Session expired, exiting')
                break
            if result.find('No such file or directory') == -1:
                logger.info('Tar file for production %s exists' % t[0])
            else:
                logger.info('Something went wrong, continue')
                continue
    
            logger.info('Going to move file from /tmp to EOS')
            cmd = 'mv /tmp/%(Soft)s_logFiles.tarz /eos/experiment/compass/%(Path)s%(Soft)s/logFiles/%(Soft)s_logFiles.tarz' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2]}
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1:
                logger.info('Session expired, exiting')
                break
            
            logger.info('Check if final tarz for %s exists on EOS' % t[0])
            cmd = 'ls /eos/experiment/compass/%(Path)s%(Soft)s/logFiles/%(Soft)s_logFiles.tarz' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2]}
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1:
                logger.info('Session expired, exiting')
                break
            if result.find('No such file or directory') == -1:
                logger.info('Tar file for production %s exists, going to copy it to Castor' % t[0])
                final_tarz_exists = True
            else:
                logger.info('Something went wrong, continue')
                continue
        
        if not final_tarz_exists:
            continue
        
        logger.info('Going to send file to Castor')        
        f_from = 'xrdcp -N -f root://eoscompass.cern.ch//eos/experiment/compass/%(Path)s%(Soft)s/logFiles/%(Soft)s_logFiles.tarz' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2]}
        f_to = 'root://castorpublic.cern.ch//castor/cern.ch/user/n/na58dst1/prodlogs/testproductions/%(Soft)s_logFiles.tarz' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2]}
        cmd = f_from + ' ' + f_to
        logger.info(cmd)
        result = exec_remote_cmd(cmd)
        if result.find('Permission denied') != -1:
            logger.info('Session expired, exiting')
            sys.exit(0)
        
        if result.succeeded:
            logger.info('Successfully sent to Castor %s' % f_from)
            task_update = Task.objects.filter(production=t[0]).update(status='archiving', date_updated=timezone.now())
            logger.info(result)
        else:
            logger.info('Error sending to Castor %s' % f_from)
            logger.error(result)
            
    logger.info('done')
