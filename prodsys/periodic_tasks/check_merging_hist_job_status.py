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

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job
from schedconfig.models import Jobsactive4, Jobsarchived4

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.check_merging_hist_job_status.log')

today = datetime.datetime.today()
logger.info('Starting %s' % __file__)

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process(__file__, pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

def main():
    logger.info('Getting tasks with status send, running and paused')
    tasks_list = Task.objects.all().filter(Q(status='send') | Q(status='running') | Q(status='paused'))
#    tasks_list = Task.objects.all().filter(name='dy2015W12t4_part6')
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    for t in tasks_list:
        logger.info('Getting jobs with merging histos status sent and running for task %s' % t.name)
        jobs_list = Job.objects.filter(task=t).filter(Q(status_merging_histos='sent') | Q(status_merging_histos='running')).values('panda_id_merging_histos', 'status_merging_histos').distinct()
        logger.info('Got list of %s jobs' % len(jobs_list))
        
        for j in jobs_list:
            j_check = None
            try:
                j_check = Jobsarchived4.objects.using('schedconfig').get(pandaid=j['panda_id_merging_histos'])
            except:
                continue
            
            if j['status_merging_histos'] != j_check.jobstatus:
                logger.info('Getting jobs for PandaID=%s' % j_check.pandaid)
                if j_check.jobstatus == 'finished' or j_check.jobstatus == 'failed' or j_check.jobstatus == 'closed':
                    today = datetime.datetime.today()
                    logger.info('Going to update jobs with PandaID=%s to status %s' % (j_check.pandaid, j_check.jobstatus))
                    if j_check.jobstatus == 'failed' or j_check.jobstatus == 'closed':
                        # refer to pilot's COMPASSExperiment PilotErrors for more details
                        if j_check.piloterrorcode == 1235 or j_check.piloterrorcode == 1237 or j_check.piloterrorcode == 1242:
                            logger.info('%s, job status will be updated to manual check is needed' % j_check.piloterrordiag)
                            jobs_list_update = Job.objects.filter(panda_id_merging_histos=j_check.pandaid).update(status_merging_histos='manual check is needed', date_updated=today)
                        else:
                            jobs_list_update = Job.objects.filter(panda_id_merging_histos=j_check.pandaid).update(status_merging_histos=j_check.jobstatus, date_updated=today)
                    elif j_check.jobstatus == 'finished':
                        jobs_list_update = Job.objects.filter(panda_id_merging_histos=j_check.pandaid).update(status_merging_histos=j_check.jobstatus, status_castor_histos='ready', date_updated=today)
    
    logger.info('done')

if __name__ == "__main__":
    sys.exit(main())
