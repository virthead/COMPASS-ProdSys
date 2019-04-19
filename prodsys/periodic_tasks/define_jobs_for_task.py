#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import datetime
from django.conf import settings
import logging
from django.core.wsgi import get_wsgi_application
from django.db import DatabaseError, IntegrityError

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from prodsys.models import Task, Job

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.define_jobs_for_task.log')

today = datetime.datetime.today()
logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process(__file__, pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

def main():
    logger.info('Getting tasks with status ready')
    td = Task.objects.all().filter(status='ready').filter(files_source='files list')
    logger.info('Got list of %s tasks' % len(td))
    for t in td:
        logger.info('Going to define jobs for %s' % t)
        
        try:
            filelist = t.filelist.splitlines()
        except:
            logger.info('Filelist is empty')
            continue
        
        logger.info('Got list of %s files' % len(filelist))
        count_added = 0
        for l in filelist:
            qs = Job.objects.filter(task=t, file=l)
            if qs.exists():
                logger.info('Such task and job already exist in the system, skipping')
                continue
            
            logger.info('Going go define a job for %s ' % l)
            runNumber = ''
            runNumber = l[l.rfind('-') + 1:l.find('.raw')]
            chunkNumber = ''
            chunkNumber = l[l.find('cdr') + 3:l.rfind('-')]
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
        if count_added == len(filelist):
            logger.info('Going to update task status to jobs defined')
            t_edit = Task.objects.get(id=t.id)
            t_edit.status = 'jobs ready'
            try:
                t_edit.save()
                logger.info('Finished processing task %s' % t)
            except Exception as e:
                logger.exception('%s (%s)' % (e.message, type(e)))
    
    logger.info('done')

if __name__ == "__main__":
    sys.exit(main())
