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
from fabric.api import env, run, execute, settings as sett, hide, put
from fabric.context_managers import shell_env, cd
import csv
import xml.etree.ElementTree as ET
import re

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.define_jobs_from_runs_mcgen.log')

today = datetime.datetime.today()
logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process('define_jobs_from_runs_mcgen.py', pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

env.hosts = []
env.hosts.append(settings.COMPASS_HOST)
env.user = settings.COMPASS_USER
env.password = settings.COMPASS_PASS

def exec_remote_cmd(cmd):
    with hide('output','running','warnings'), sett(warn_only=True):
        return run(cmd)

def define_jobs_from_runs_mcgen():
    logger.info('Getting MC generation tasks with status ready')
    tasks_list = Task.objects.filter(type='MC generation').filter(status='ready')
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    for t in tasks_list:
        logger.info('Going to read on cvmfs settings file for task %s' % t.name)
        if t.template == 'template_mu+.opt':
            settings_suffix = 'mu+'
        else:
            settings_suffix = 'mu-'
            
        settings_file = t.home + t.path + t.soft + '/settings_' + str(t.year) + str(t.period) + '_' + settings_suffix + '.xml'
        logger.info('Settings file must be stored on the following path: %s' % settings_file)
        logger.info('Going to read .xml settings file')
        cmd = "cat %s" % (settings_file)
        logger.info(cmd)
        result = exec_remote_cmd(cmd)
        logger.info(result)
        if result.find('Permission denied') != -1:
            logger.info('Session expired, exiting')
            break
        
        settings_xml = ET.fromstring(result)
        logger.info(settings_xml)
        
        path = settings.EOS_HOME + 'mc/' + t.path + t.soft + '/xmls/'
        logger.info('Going to check if path %s exists, if not, create it' % path)
        cmd = "mkdir -p %s" % path
        logger.info(cmd)
        result = exec_remote_cmd(cmd)
        logger.info(result)
        if result.find('Permission denied') != -1:
            logger.info('Session expired, exiting')
            break
        
        logger.info('Going to extract runs and chunks from filelist field of task %s' % t.name)
        logger.info(t.filelist)
        
        reader = csv.DictReader(t.filelist.splitlines(), delimiter = ',', skipinitialspace = True, fieldnames = ['run_number', 'number_of_chunks'])
        chunks_to_generate = 0
        chunks_generated = 0
        for r in reader:
            logger.info('Going to generate %s xmls for run %s' % (r['number_of_chunks'], r['run_number']))
            chunks_to_generate = chunks_to_generate + int(r['number_of_chunks'])
            i = 1
            while i <= int(r['number_of_chunks']):
                file_name = 'mcr' + format(i, '05d') + '-' + r['run_number']
                file_name_dat = file_name + '.dat'
                file_name_xml = file_name + '.xml'
                
                logger.info('Going to generate %s chunk for run %s' % (i, r['run_number']))
                new_settings_xml = settings_xml
                new_path = path + file_name_xml
                for seed in new_settings_xml.iter('seed'):
                    seed.text = str(i)
                for runName in new_settings_xml.iter('runName'):
                    runName.text = file_name
                if t.use_local_generator_file == 'yes':
                    for localGeneratorFile in new_settings_xml.iter('localGeneratorFile'):
                        localGeneratorFile.text = './' + file_name_dat
#                    localGeneratorFile.text = settings.EOS_HOME + 'mc/' + t.path + t.soft + '/o_data/' + 'mcr00001-274495.dat' # + tail
                for eventsPerChunk in new_settings_xml.iter('eventsPerChunk'):
                    number_of_events = eventsPerChunk.text
                for outputPath in new_settings_xml.iter('outputPath'):
                    outputPath.text = './'
                
#                logger.info(ET.tostring(new_settings_xml, 'utf-8').decode())
                
                logger.info('Going to save generated file %s at /tmp' % file_name_xml)
                new_settings_data = '<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n' + ET.tostring(new_settings_xml)
                new_settings_data = re.sub(r"\n+", "\n", new_settings_data)
                f = open('/tmp/%s' % file_name_xml, 'w')
                f.write(new_settings_data)
                f.close()
                
                logger.info('Uploading file %s to %s' % (file_name_xml, new_path))
                try:
                    put('/tmp/%s' % file_name_xml, new_path)
                except:
                    logger.info('Session expired, exiting')
                    break
                
                logger.info('Going to check if file %s exists on EOS' % file_name_xml)
                cmd = 'ls %(fileName)s' % {'fileName': new_path}
                logger.info(cmd)
                result = exec_remote_cmd(cmd)
                logger.info(result)
                if result.find('Permission denied') != -1:
                    logger.info('Session expired, exiting')
                    break
                if result.find('No such file or directory') != -1:
                    logger.info('File upload failed, exiting')
                    break
                
                logger.info('%s was uploaded successfully, going to create a job for the file' % file_name_xml)
                
                qs = Job.objects.filter(task=t, file=new_path)
                if qs.exists():
                    logger.info('Such task and job already exist in the system, skipping')
                    continue
                
                j = Job(
                    task = t,
                    file = new_path,
                    status = 'staged',
                    number_of_events = number_of_events,
                    run_number = r['run_number'],
                    chunk_number = i,
                    date_added = today,
                    date_updated = today
                )
                try:
                    j.save()
                    logger.info('Saved job for %s' % file_name_xml)
                    chunks_generated += 1
                except IntegrityError as e:
                    logger.exception('Unique together catched, was not saved')
                except DatabaseError as e:
                    logger.exception('Something went wrong while saving: %s' % e.message)
                
                i += 1
                
        logger.info('Chunks to generate: %s' % chunks_to_generate)
        logger.info('Chunks generated: %s' % chunks_generated)
        if chunks_to_generate == chunks_generated:
            logger.info('Going to update task status to jobs defined')
            t_edit = Task.objects.get(id=t.id)
            t_edit.status = 'jobs ready'
            try:
                t_edit.save()
                logger.info('Finished processing task %s' % t)
            except Exception as e:
                logger.exception('%s (%s)' % (e.message, type(e)))
    
    logger.info('done')
