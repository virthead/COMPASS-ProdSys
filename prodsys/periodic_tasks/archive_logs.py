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
    session_expired = False
    
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
    
    logger.info('Getting tasks with status archive')
    tasks_list = list(Task.objects.all().exclude(Q(site='BW_COMPASS_MCORE') | Q(site='BW_STAMPEDE_MCORE') | Q(site='BW_FRONTERA_MCORE')).filter(status='archive').values_list('production', 'path', 'soft', 'type', 'year').distinct()[:5])
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
        
        runs_tarred = 0
        for run_number in runs_list:
            if runs_tarred >= runs_to_tar:
                logger.info('Limit of runs to tar has reached, breaking')
                break
            
            pars = {'Prod': t[0], 'Path': t[1], 'Soft': t[2], 'run_number': run_number, 'eosHome': settings.EOS_HOME}

            logger.info('Going to tar run %s' % run_number)
            cmd = 'tar -cvzf /tmp/%(Prod)s.%(run_number)s.tar %(eosHome)s%(Path)s%(Soft)s/logFiles/%(Prod)s.*%(run_number)s-*.gz' % pars
            if t[3] == 'MC generation' or t[3] == 'MC reconstruction':
                cmd = 'tar -cvzf /tmp/%(Prod)s.%(run_number)s.tar %(eosHome)smc/%(Path)s%(Soft)s/logFiles/%(Prod)s.*%(run_number)s*.gz' % pars
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1 or result.find('Input/output error') != -1:
                logger.info('Error, exiting')
                access_denied = True
                break
            
            logger.info('Going to check if tar for run %s exists in /tmp' % run_number)
            cmd = 'ls /tmp/%(Prod)s.%(run_number)s.tar' % pars
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1 or result.find('Input/output error') != -1:
                logger.info('Error, exiting')
                access_denied = True
                break
            if result.find('No such file or directory') == -1:
                logger.info('Tar file for run %s exists' % run_number)
            else:
                logger.info('Something went wrong, continue')
                continue
            
            logger.info('Going to move file from /tmp to EOS')
            cmd = 'mv /tmp/%(Prod)s.%(run_number)s.tar %(eosHome)s' % pars
            if t[3] == 'MC generation' or t[3] == 'MC reconstruction':
                cmd += 'mc/'
            cmd += '%(Path)s%(Soft)s/logFiles/%(Prod)s.%(run_number)s.tar' % pars
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1 or result.find('Input/output error') != -1:
                logger.info('Error, exiting')
                access_denied = True
                break
            
            logger.info('Going to check if tar for run %s exists on EOS' % run_number)
            path = '%(eosHome)s' % pars
            if t[3] == 'MC generation' or t[3] == 'MC reconstruction':
                path += 'mc/'
            path += '%(Path)s%(Soft)s/logFiles/' % pars
            file = '%(Prod)s.%(run_number)s.tar' % pars
            cmd = 'ls -al %s%s' % (path, file)
            logger.info(cmd)
            result = exec_remote_cmd(cmd)
            logger.info(result)
            if result.find('Permission denied') != -1 or result.find('Input/output error') != -1:
                logger.info('Error, exiting')
                access_denied = True
                break
            if result.find('No such file or directory') == -1:
                logger.info('Tar file for run %s exists, going to check size, update chunks and continue' % run_number)
                reader = csv.DictReader(result.splitlines(), delimiter = ' ', skipinitialspace = True, fieldnames = ['permissions', 'links', 'owner', 'group', 'size', 'date1', 'date2', 'time', 'name'])
                good_file = False
                for r in reader:
                    if r['name'].find(file) != -1:
                        if r['size'] == '0':
                            logger.info('File has zero size, needs to be re-generated')
                        else:
                            good_file = True
                            jobs_update = Job.objects.filter(task__production=t[0]).filter(run_number=run_number).update(status_logs_archived='yes', date_updated=timezone.now())
                if not good_file:
                    continue
            else:
                logger.info('Something went wrong, continue')
                continue
                
            runs_tarred += 1
        
        logger.info('%s of %s runs of production %s were archived and moved to EOS' % (runs_tarred, len(runs_list), t[0])) 
        
        if len(runs_list) > 0:
            continue
        
        logger.info('All runs of %s are in tars, going to check if empty files were generated' % t[0])
        cmd = 'ls -al %(eosHome)s%(Path)s%(Soft)s/logFiles/%(Prod)s.*.tar' % {'eosHome': settings.EOS_HOME, 'Prod': t[0], 'Path': t[1], 'Soft': t[2]}
        if t[3] == 'MC generation' or t[3] == 'MC reconstruction':
            cmd = 'ls -al %(eosHome)smc/%(Path)s%(Soft)s/logFiles/%(Prod)s.*.tar' % {'eosHome': settings.EOS_HOME, 'Prod': t[0], 'Path': t[1], 'Soft': t[2]}
        logger.info(cmd)
        result = exec_remote_cmd(cmd)
        logger.info(result)
        if result.find('Permission denied') != -1 or result.find('Input/output error') != -1:
            logger.info('Error, exiting')
            access_denied = True
            break
          
        if not result.succeeded:
            logger.info('Error reading directory, skipping')
            continue
        
        reader = csv.DictReader(result.splitlines(), delimiter = ' ', skipinitialspace = True, fieldnames = ['permissions', 'links', 'owner', 'group', 'size', 'date1', 'date2', 'time', 'name'])
        empty_file_found = False
        for r in reader:
            if r['size'] == '0':
                run_number = r['name'][r['name'].find('.') + 1:r['name'].find('.tar')]
                logger.info('Empy file found for run: %s, going to resend archiving' % run_number)
                jobs_update = Job.objects.filter(task__production=t[0]).filter(run_number=run_number).update(status_logs_archived='no', date_updated=timezone.now())
                empty_file_found = True
        
        if empty_file_found:
            continue
        
        logger.info('Ready to create tar for production %s' % t[0])
        
        logger.info('Going to create final tarz file for production %s' % t[0])
        if t[3] == 'mass production':
            cmd = 'tar -cvzf /tmp/%(Prod)s_logFiles.tarz %(eosHome)s%(Path)s%(Soft)s/logFiles/%(Prod)s.*.tar' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2], 'eosHome': settings.EOS_HOME}
        elif t[3] == 'MC generation' or t[3] == 'MC reconstruction':
            cmd = 'tar -cvzf /tmp/%(Soft)s_logFiles.tarz %(eosHome)smc/%(Path)s%(Soft)s/logFiles/%(Prod)s.*.tar' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2], 'eosHome': settings.EOS_HOME}
        else:
            cmd = 'tar -cvzf /tmp/%(Soft)s_logFiles.tarz %(eosHome)s%(Path)s%(Soft)s/logFiles/%(Prod)s.*.tar' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2], 'eosHome': settings.EOS_HOME}
        logger.info(cmd)
        result = exec_remote_cmd(cmd)
        logger.info(result)
        if result.find('Permission denied') != -1 or result.find('Input/output error') != -1:
            logger.info('Error, exiting')
            access_denied = True
            break
          
        if not result.succeeded:
            logger.info('Error generating archive, skipping')
            continue
        
        logger.info('Going to check if final tar for production %s exists in /tmp' % t[0])
        if t[3] == 'mass production':
            cmd = 'ls /tmp/%(Prod)s_logFiles.tarz' % {'Prod': t[0]}
        else:
            cmd = 'ls /tmp/%(Soft)s_logFiles.tarz' % {'Soft': t[2]}
        logger.info(cmd)
        result = exec_remote_cmd(cmd)
        logger.info(result)
        if result.find('Permission denied') != -1 or result.find('Input/output error') != -1:
            logger.info('Error, exiting')
            access_denied = True
            break
        if result.find('No such file or directory') == -1:
            logger.info('Tar file for production %s exists' % t[0])
        else:
            logger.info('Something went wrong, continue')
            continue

        logger.info('Going to move file from /tmp to EOS')
        if t[3] == 'mass production':
            cmd = 'mv /tmp/%(Prod)s_logFiles.tarz %(eosHome)s%(Path)s%(Soft)s/logFiles/%(Prod)s_logFiles.tarz' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2], 'eosHome': settings.EOS_HOME}
        elif t[3] == 'MC generation' or t[3] == 'MC reconstruction':
            cmd = 'mv /tmp/%(Soft)s_logFiles.tarz %(eosHome)smc/%(Path)s%(Soft)s/logFiles/%(Soft)s_logFiles.tarz' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2], 'eosHome': settings.EOS_HOME}
        else:
            cmd = 'mv /tmp/%(Soft)s_logFiles.tarz %(eosHome)s%(Path)s%(Soft)s/logFiles/%(Soft)s_logFiles.tarz' % {'Prod': t[0], 'Path': t[1], 'Soft': t[2], 'eosHome': settings.EOS_HOME}
        logger.info(cmd)
        result = exec_remote_cmd(cmd)
        logger.info(result)
        if result.find('Permission denied') != -1 or result.find('Input/output error') != -1:
            logger.info('Error, exiting')
            access_denied = True
            break
        
        logger.info('Check if final tarz for %s exists on EOS' % t[0])
        path = '%(eosHome)s%(Path)s%(Soft)s/logFiles/' % {'eosHome': settings.EOS_HOME, 'Path': t[1], 'Soft': t[2]}
        if t[3] == 'MC generation' or t[3] == 'MC reconstruction':
            path = '%(eosHome)smc/%(Path)s%(Soft)s/logFiles/' % {'eosHome': settings.EOS_HOME, 'Path': t[1], 'Soft': t[2]}
        if t[3] == 'mass production':
            file = '%(Prod)s_logFiles.tarz' % {'Prod': t[0]}
        else:
            file = '%(Soft)s_logFiles.tarz' % {'Soft': t[2]}
        cmd = 'ls -al %s%s' % (path, file)
        logger.info(cmd)
        result = exec_remote_cmd(cmd)
        logger.info(result)
        if result.find('Permission denied') != -1 or result.find('Input/output error') != -1:
            logger.info('Error, exiting')
            access_denied = True
            break
        if result.find('No such file or directory') == -1:
            logger.info('Tar file for production %s exists, going to check size and copy it to cta' % t[0])
            reader = csv.DictReader(result.splitlines(), delimiter = ' ', skipinitialspace = True, fieldnames = ['permissions', 'links', 'owner', 'group', 'size', 'date1', 'date2', 'time', 'name'])
            good_file = False
            for r in reader:
                if r['name'].find(file) != -1:
                    if r['size'] == '0':
                        logger.info('File has zero size, needs to be re-generated')
                    else:
                        good_file = True
            if not good_file:
                continue
        else:
            logger.info('Something went wrong, continue')
            continue
        
        logger.info('Going to send file to cta')        
        p_from = 'fts-transfer-submit -s %(ftsServer)s -o %(eosHomeRoot)s%(eosHome)s' % {'ftsServer': settings.FTS_SERVER, 'eosHomeRoot':settings.EOS_HOME_ROOT, 'eosHome': settings.EOS_HOME}
        if t[3] == 'MC generation' or t[3] == 'MC reconstruction':
            p_from += 'mc/'
        p_from += '%(Path)s%(Soft)s/logFiles/' % {'Path': t[1], 'Soft': t[2]}
        if t[3] == 'mass production':
            f_name = '%(Prod)s_logFiles.tarz' % {'Prod': t[0]}
            p_to = '%(ctaHomeRoot)s%(ctaHomeLogs)s%(Year)s/' % {'ctaHomeRoot': settings.CTA_HOME_ROOT, 'ctaHomeLogs': settings.CTA_HOME_LOGS, 'Year': t[4]}
        elif t[3] == 'MC generation':
            f_name = '%(Soft)s_logFiles.tarz' % {'Soft': t[2]}
            p_to = '%(ctaHomeRoot)s%(ctaHomeLogs)smc_prod/gen/' % {'ctaHomeRoot': settings.CTA_HOME_ROOT, 'ctaHomeLogs': settings.CTA_HOME_LOGS}
        elif t[3] == 'MC reconstruction':
            f_name = '%(Soft)s_logFiles.tarz' % {'Soft': t[2]}
            p_to = '%(ctaHomeRoot)s%(ctaHomeLogs)smc_prod/reco/' % {'ctaHomeRoot': settings.CTA_HOME_ROOT, 'ctaHomeLogs': settings.CTA_HOME_LOGS}
        else:
            f_name = '%(Soft)s_logFiles.tarz' % {'Soft': t[2]}
            p_to = '%(ctaHomeRoot)s%(ctaHomeLogs)stestproductions/' % {'ctaHomeRoot': settings.CTA_HOME_ROOT, 'ctaHomeLogs': settings.CTA_HOME_LOGS}
        
        cmd = p_from + f_name + ' ' + p_to + f_name
        logger.info(cmd)
        result = exec_remote_cmd(cmd)
        if result.find('Permission denied') != -1 or result.find('Input/output error') != -1:
            logger.info('Error, exiting')
            sys.exit(0)
         
        if result.succeeded:
            logger.info('Successfully sent to cta %s' % f_name)
            task_update = Task.objects.filter(production=t[0]).update(status='archiving', date_updated=timezone.now())
            logger.info(result)
        else:
            logger.info('Error sending to cta %s' % f_name)
            logger.error(result)
            
    logger.info('done')
