#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import commands
from django.utils import timezone
from django.conf import settings
import logging
from django.core.wsgi import get_wsgi_application
from django.db import DatabaseError, IntegrityError

import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.define_jobs_from_runs_mcreco.log')

logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process(__file__, pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

def main():
    logger.info('Getting tasks with status ready')
    tasks_list = Task.objects.all().filter(type='MC reconstruction').filter(status='ready').exclude(parent_task__isnull=True)
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    for t in tasks_list:
        chunks_to_generate = 0
        chunks_generated = 0

        logger.info('Going to define jobs for %s' % t)
        
        logger.info('Going to get jobs from the parent task %s' % t.parent_task)
        jobs_list = Job.objects.all().filter(task=t.parent_task).order_by('id')
        logger.info('Got list of %s jobs' % len(jobs_list))
        chunks_to_generate = len(jobs_list)
        for pj in jobs_list:
            file = '%(castorHome)smc_prod/CERN/%(Year)s/%(Period)s/%(prodSoft)s/mcgen/mcr%(chunkNumber)s-%(runNumber)s_run000.tgeant' % {'castorHome': settings.CASTOR_HOME, 'Year': t.year, 'Period': t.period, 'prodPath': t.path, 'prodSoft': t.soft, 'chunkNumber': format(pj.chunk_number, '05d'), 'runNumber': pj.run_number}
            j = Job(
                task = t,
                file = file,
                number_of_events = pj.number_of_events,
                run_number = pj.run_number,
                chunk_number = pj.chunk_number,
                date_added = timezone.now(),
                date_updated = timezone.now()
            )
            try:
                j.save()
                logger.info('Saved job for %s' % file)
                chunks_generated += 1
            except IntegrityError as e:
                logger.exception('Unique together catched, was not saved')
            except DatabaseError as e:
                logger.exception('Something went wrong while saving: %s' % e.message)
        
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

if __name__ == "__main__":
    sys.exit(main())
