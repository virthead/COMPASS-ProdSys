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
getRotatingFileHandler(logger, 'periodic_tasks.define_jobs_from_runs.log')

today = datetime.datetime.today()
logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
logger.info('__file__: %s' % __file__)

if check_process("define_jobs_from_runs.py", pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

env.hosts = []
env.hosts.append(settings.COMPASS_HOST)
env.user = settings.COMPASS_USER
env.password = settings.COMPASS_PASS

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def define_jobs_from_runs():
    with cd('/tmp'):
        years_in_text_files = [2015, 2016, 2017, 2018]
        
        logger.info('Getting tasks with status ready')
        tasks_list = Task.objects.all().filter(Q(type='test production') | Q(type='mass production') | Q(type='technical production') | Q(type='DDD filtering')).filter(status='ready').filter(files_source='runs list')
        logger.info('Got list of %s tasks' % len(tasks_list))
        
        for t in tasks_list:
            logger.info('Going to define jobs for %s' % t)
            try:
                runs_list = t.filelist.splitlines()
            except:
                logger.info('Runs list is empty')
                continue
            
            logger.info('Got list of %s runs' % len(runs_list))
            
            if t.year in years_in_text_files:
                logger.info('Text files with data exist for %s year' % t.year)
                
                lines_read = 0
                count_added = 0
                for r in runs_list:
                    r = r.strip()
                    cmd = 'grep -rnw /cvmfs/compass.cern.ch/production/ORACLE_DB_TO_TXT/%s* -e \'%s.raw\'' % (t.year, r)
                    logger.info(cmd)
                    result = exec_remote_cmd(cmd)
                    logger.info(result)
                    if result.succeeded:
                        logger.info('Files list was generated')
                        reader = csv.DictReader(result.splitlines(), delimiter = ' ', skipinitialspace = True, fieldnames = ['pattern', 'run_number', 'name', 'events'])
                        
                        reader_lines_count = int(sum(1 for row in csv.DictReader(result.splitlines()))) + 1
                        logger.info('Got list of %s files' % reader_lines_count)
                        lines_read += reader_lines_count
                        for l in reader:
                            l['name'] = l['name'].replace("\t", "").replace("\t", "")
                            logger.info('Going go define a job for %s ' % l['name'])
                            runNumber = 0
                            try:
                                runNumber = int(l['name'][l['name'].find('-') + 1:l['name'].find('.raw')])
                            except:
                                logger.info('Run number is not integer, skipping')
                                continue
                            
                            logger.info('runNumber: %s' % runNumber)
                            
                            chunkNumber = 0
                            try:
                                chunkNumber = int(l['name'][l['name'].find('cdr') + 3:l['name'].find('-')])
                            except:
                                logger.info('Chunk number is not integer, skipping')
                                continue
                            logger.info('chunkNumber: %s' % chunkNumber)
                            
                            number_of_events = -1
                            try:
                                number_of_events = int(l['events'])
                            except:
                                logger.info('Number of events %s is not integer, setting -1' % l['events'])
                            
                            if number_of_events == 0:
                                logger.info('0 events in the file, skipping')
                                continue
                            
                            logger.info('Check that task and job are unique')
                            
                            if t.site == 'BW_COMPASS_MCORE':
                                bw_file = t.files_home + l['name'][l['name'].rfind('/')+1:]
                                logger.info('File for BlueWaters task was changed to %s' % bw_file)
                                
                                qs = Job.objects.filter(task=t, file=bw_file)
                                if qs.exists():
                                    logger.info('Such task and job already exist in the system, skipping')
                                    continue
                                
                                j = Job(
                                    task = t,
                                    file = bw_file,
                                    number_of_events = number_of_events,
                                    run_number = runNumber,
                                    chunk_number = chunkNumber,
                                    date_added = today,
                                    date_updated = today
                                )
                            else:
                                qs = Job.objects.filter(task=t, file=l['name'])
                                if qs.exists():
                                    logger.info('Such task and job already exist in the system, skipping')
                                    continue
                                
                                j = Job(
                                    task = t,
                                    file = l['name'],
                                    number_of_events = number_of_events,
                                    run_number = runNumber,
                                    chunk_number = chunkNumber,
                                    date_added = today,
                                    date_updated = today
                                )
                            try:
                                j.save()
                                logger.info('Saved job for %s' % l)
                                count_added += 1
                            except IntegrityError as e:
                                logger.exception('Unique together catched, was not saved')
                            except DatabaseError as e:
                                logger.exception('Something went wrong while saving: %s' % e.message)
                    
                logger.info('Lines read %s' % lines_read)
                logger.info('Added %s jobs' % count_added)
                if count_added == lines_read:
                    logger.info('Going to update task status to jobs defined')
                    t_edit = Task.objects.get(id=t.id)
                    t_edit.status = 'jobs ready'
                    try:
                        t_edit.save()
                        logger.info('Finished processing task %s' % t)
                    except Exception as e:
                        logger.exception('%s (%s)' % (e.message, type(e)))
            else:
                lines_read = 0
                count_added = 0
                for r in runs_list:
                    cmd0 = 'cd /tmp'
                    logger.info('Going to change dir to /tmp')
                    result0 = exec_remote_cmd(cmd0)
                    if not result0.succeeded:
                        logger.error('Error changing dir to /tmp')
                        continue
                    
                    r = r.strip()
                    cmd = '/eos/user/n/na58dst1/production/GetFileList.pl %s' % r
                    logger.info(cmd)
                    result = exec_remote_cmd(cmd)
                    logger.info(result)
                    if result.find(' found for run %s in the DB. (see file Run_%s.list)' % (r, r)) != -1:
                        logger.info('Files list was generated')
                        cmd1 = 'cat Run_%s.list' % r
                        logger.info(cmd1)
                        result1 = exec_remote_cmd(cmd1)
                        logger.info(result)
                        if result1.succeeded:
                            logger.info('Successfully got list of files for run %s' % r)
                            logger.info(result1)
                        else:
                            logger.info('Error getting list of files for run %s' % r)
                            logger.error(result1)
                            continue
                        
                        fileslist = result1.splitlines()
                        logger.info('Got list of %s files' % len(fileslist))
                        lines_read += len(fileslist)
                        for l in fileslist:
                            logger.info('Going go define a job for %s ' % l)
                            runNumber = ''
                            runNumber = l[l.find('-') + 1:l.find('.raw')]
                            chunkNumber = ''
                            chunkNumber = l[l.find('cdr') + 3:l.find('-')]
                            if t.site == 'BW_COMPASS_MCORE':
                                bw_file = t.files_home + l[l.rfind('/')+1:]
                                logger.info('File for BlueWaters task was changed to %s' % bw_file)
                                qs = Job.objects.filter(task=t, file=bw_file)
                                if qs.exists():
                                    logger.info('Such task and job already exist in the system, skipping')
                                    continue
                                
                                j = Job(
                                    task = t,
                                    file = bw_file,
                                    run_number = runNumber,
                                    chunk_number = chunkNumber,
                                    date_added = today,
                                    date_updated = today
                                )
                            else:
                                qs = Job.objects.filter(task=t, file=l)
                                if qs.exists():
                                    logger.info('Such task and job already exist in the system, skipping')
                                    continue
                                
                                j = Job(
                                    task = t,
                                    file = l,
                                    run_number = runNumber,
                                    chunk_number = chunkNumber,
                                    date_added = today,
                                    date_updated = today
                                )
                            try:
                                j.save()
                                logger.info('Saved job for %s' % l)
                                count_added += 1
                            except IntegrityError as e:
                                logger.exception('Unique together catched, was not saved')
                            except DatabaseError as e:
                                logger.exception('Something went wrong while saving: %s' % e.message)
                    
                logger.info('Added %s jobs' % count_added)
                if count_added == lines_read:
                    logger.info('Going to update task status to jobs defined')
                    t_edit = Task.objects.get(id=t.id)
                    t_edit.status = 'jobs ready'
                    try:
                        t_edit.save()
                        logger.info('Finished processing task %s' % t)
                    except Exception as e:
                        logger.exception('%s (%s)' % (e.message, type(e)))
        
    logger.info('done')
