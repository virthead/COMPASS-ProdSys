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
getRotatingFileHandler(logger, 'periodic_tasks.send_cta_jobs_dump.log')

logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
logger.info('__file__: %s' % __file__)

if check_process("send_cta_jobs_dump.py", pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

env.hosts = []
env.hosts.append(settings.COMPASS_HOST)
env.user = settings.COMPASS_USER
env.password = settings.COMPASS_PASS

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def copy_to_cta():
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
        logger.info('Getting run numbers for task %s with status finished and status merging evntdmp finished and status merging histos finished and status castor evntdmp ready' % t.name)
        runs_list = Job.objects.filter(task=t).filter(status_castor_evntdmp='ready').exclude(chunk_number_merging_evntdmp=-1).filter(attempt_castor_evntdmp__lt=t.max_attempts).filter(status_castor_evntdmp='ready').order_by('run_number').values_list('run_number', flat=True).distinct()
        logger.info('Got list of %s runs' % len(runs_list))
        
        for r in runs_list:
            copy_list = []
            logger.info('Getting chunk numbers for run %s' % r)
            merged_chunks_list = Job.objects.filter(task=t).filter(run_number=r).filter(status_castor_evntdmp='ready').exclude(chunk_number_merging_evntdmp=-1).order_by('chunk_number_merging_evntdmp').values_list('chunk_number_merging_evntdmp', flat=True).distinct()
            logger.info('Got list of %s chunks' % len(merged_chunks_list))
            if len(merged_chunks_list) == 0:
                logger.info('No chunks found for archiving')
                continue
            
            logger.info('Going to build copy list')
            for c in merged_chunks_list:
                tapesHomeRoot = '%(ctaHomeRoot)s%(ctaHome)s' % {'ctaHomeRoot': settings.CTA_HOME_ROOT, 'ctaHome': settings.CTA_HOME}
                
                f_from = 'fts-transfer-submit -s %(ftsServer)s -o %(eosHomeRoot)s%(eosHome)s%(prodPath)s%(prodSoft)s/mergedDump/slot%(prodSlt)s/evtdump%(prodSlt)s-%(runNumber)s.raw' % {'prodPath': t.path, 'prodSoft': t.soft, 'runNumber': r, 'prodSlt': t.prodslt, 'phastVer': t.phastver, 'ftsServer': settings.FTS_SERVER, 'eosHomeRoot':settings.EOS_HOME_ROOT, 'eosHome': settings.EOS_HOME}
                if format(c, '03d') != '000':
                    f_from = f_from + '.' + format(c, '03d')
                
                oracle_dst = ''
                if t.type == 'mass production':
                    oracle_dst = '/oracle_dst/'
                
                f_to = '%(tapesHomeRoot)s%(prodPath)s%(oracleDst)s%(prodSoft)s/mergedDump/slot%(prodSlt)s/evtdump%(prodSlt)s-%(runNumber)s.raw' % {'tapesHomeRoot': tapesHomeRoot, 'prodPath': t.path, 'prodSoft': t.soft, 'runNumber': r, 'prodSlt': t.prodslt, 'phastVer': t.phastver, 'oracleDst': oracle_dst}
                if format(c, '03d') != '000':
                    f_to = f_to + '.' + format(c, '03d')
                
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
                    logger.info('Successfully sent to castor run number %s merging chunk number %s' % (r, chunk))
                    jobs_update = Job.objects.filter(task=t).filter(run_number=r).filter(chunk_number_merging_evntdmp=chunk).update(status_castor_evntdmp='sent', attempt_castor_evntdmp=1, date_updated=timezone.now())
                    logger.info(result)
                else:
                    logger.info('Error sending to castor run number %s merging chunk number %s' % (r, chunk))
                    logger.error(result)
                    
                    if result.find('No such file or directory') != -1:
                        logger.info('File dissapeared from EOS, going to resend merging of dump for run number %s' % r)
                        jobs_list = Job.objects.filter(task=t).filter(run_number=r).update(status_merging_evntdmp='ready', chunk_number_merging_evntdmp=-1, status_x_check_evntdmp='no', status_castor_evntdmp=None, date_updated=timezone.now())
    
    logger.info('done')
