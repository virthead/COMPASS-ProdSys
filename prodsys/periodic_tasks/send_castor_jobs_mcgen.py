#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import commands
from django.utils import timezone
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

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.send_castor_jobs_mcgen.log')

logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
logger.info('__file__: %s' % __file__)

if check_process("send_castor_jobs_mcgen.py", pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

env.hosts = []
env.hosts.append(settings.COMPASS_HOST)
env.user = settings.COMPASS_USER
env.password = settings.COMPASS_PASS

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def copy_to_castor():
    logger.info('Going to prepare an environment')
    cmd = 'export LC_ALL=C; unset LANGUAGE;'
    logger.info(cmd)
    result = exec_remote_cmd(cmd)
    if result.find('Permission denied') != -1 or result.find('open denied') != -1:
        logger.info('Session expired, exiting')
        session_expired = True
    
    if result.succeeded:
        logger.info('Successfully set the environment')
        logger.info(result)
    else:
        logger.info('Error setting an environment')
        logger.error(result)
    
    logger.info('Going to prepare an environment')
    cmd = 'setenv LC_ALL C; unset LANGUAGE;'
    logger.info(cmd)
    result = exec_remote_cmd(cmd)
    if result.find('Permission denied') != -1 or result.find('open denied') != -1:
        logger.info('Session expired, exiting')
        session_expired = True
    
    if result.succeeded:
        logger.info('Successfully set the environment')
        logger.info(result)
    else:
        logger.info('Error setting an environment')
        logger.error(result)
    
    logger.info('Going to create a proxy')
    cmd = settings.VOMS_PROXY_INIT
    logger.info(cmd)
    result = exec_remote_cmd(cmd)
    if result.find('Permission denied') != -1 or result.find('open denied') != -1:
        logger.info('Session expired, exiting')
        session_expired = True
    
    if result.succeeded:
        logger.info('Successfully created new proxy')
        logger.info(result)
    else:
        logger.info('Error creating a proxy')
        logger.error(result)
    
    logger.info('Getting tasks with status send and running')
    tasks_list = Task.objects.all().exclude(Q(site='BW_COMPASS_MCORE') | Q(site='STAMPEDE_COMPASS_MCORE') | Q(site='FRONTERA_COMPASS_MCORE')).filter(Q(status='send') | Q(status='running'))
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    for t in tasks_list:
        logger.info('Getting jobs for task %s with status finished and status castor mcgen ready' % t.name)
        runs_list = Job.objects.filter(task=t).filter(status_castor_mcgen='ready').filter(attempt_castor_mcgen__lt=t.max_attempts).filter(status_castor_mcgen='ready').order_by('run_number').values_list('run_number', flat=True).distinct()
        logger.info('Got list of %s runs' % len(runs_list))
        
        for r in runs_list:
            copy_list = []
            logger.info('Getting chunk numbers for run %s' % r)
            chunks_list = Job.objects.filter(task=t).filter(run_number=r).filter(status_castor_mcgen='ready').order_by('chunk_number').values_list('chunk_number', flat=True).distinct()
            logger.info('Got list of %s chunks' % len(chunks_list))
            if len(chunks_list) == 0:
                logger.info('No chunks found for archiving')
                continue
            
            logger.info('Going to build copy list')
            for c in chunks_list:
                if t.tapes_backend == 'castor':
                    tapesHomeRoot = '%(castorHomeRoot)s%(castorHome)s' % {'castorHomeRoot': settings.CASTOR_HOME_ROOT, 'castorHome': settings.CASTOR_HOME}
                else:
                    tapesHomeRoot = '%(ctaHomeRoot)s%(ctaHome)s' % {'ctaHomeRoot': settings.CTA_HOME_ROOT, 'ctaHome': settings.CTA_HOME}
                    
                f_from = 'fts-transfer-submit -s %(ftsServer)s -o %(eosHomeRoot)s%(eosHome)smc/%(prodPath)s%(prodSoft)s/mcgen/mcr%(chunkNumber)s-%(runNumber)s_run000.tgeant' % {'prodPath': t.path, 'prodSoft': t.soft, 'chunkNumber': format(c, '05d'), 'runNumber': r, 'ftsServer': settings.FTS_SERVER, 'eosHomeRoot':settings.EOS_HOME_ROOT, 'eosHome': settings.EOS_HOME}
                f_to = '%(tapesHomeRoot)smc_prod/CERN/%(Year)s/%(Period)s/%(prodSoft)s/mcgen/mcr%(chunkNumber)s-%(runNumber)s_run000.tgeant' % {'tapesHomeRoot': tapesHomeRoot, 'Year': t.year, 'Period': t.period, 'prodPath': t.path, 'prodSoft': t.soft, 'chunkNumber': format(c, '05d'), 'runNumber': r}
                
                f = f_from + ' ' + f_to
                copy_list.append([c, f])
            
            logger.info('List prepared, going to execute copy commands')
            for l in copy_list:
                chunk = l[0]
                cmd = l[1] 
                logger.info('%s:%s' % (chunk, cmd))
                result = exec_remote_cmd(cmd)
                if result.find('Permission denied') != -1:
                    logger.info('Session expired, exiting')
                    sys.exit(0)
                
                if result.succeeded:
                    logger.info('Successfully sent to castor run number %s chunk number %s' % (r, chunk))
                    jobs_update = Job.objects.filter(task=t).filter(run_number=r).filter(chunk_number=chunk).update(status_castor_mcgen='sent', attempt_castor_mcgen=1, date_updated=timezone.now())
                    logger.info(result)
                else:
                    logger.info('Error sending to castor run number %s chunk number %s' % (r, chunk))
                    logger.error(result)
                    
                    if result.find('No such file or directory') != -1:
                        logger.info('File dissapeared from EOS, going to resend merging it for run number %s and chunk %s' % (r, chunk))
                        jobs_list = Job.objects.filter(task=t).filter(run_number=r).filter(chunk_number=chunk).update(status='failed', status_castor_mcgen=None, date_updated=timezone.now())
    
    logger.info('done')
