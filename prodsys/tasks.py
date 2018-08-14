# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from celery import shared_task
from django.conf import settings
import logging

import sys, os
import time 
import datetime
from django.db import DatabaseError, IntegrityError
from _mysql import NULL

import saga
from userinterface import Client
import taskbuffer
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from prodsys.periodic_tasks.utils import getRotatingFileHandler

aSrvID = None
site = 'CERN_COMPASS_PROD'
os.environ["PANDA_URL_SSL"] = settings.PANDA_URL_SSL
os.environ["PANDA_URL"] = settings.PANDA_URL
os.environ["X509_USER_PROXY"] = settings.X509_USER_PROXY

from django.db.models import Q
from prodsys.models import Task, Job

@shared_task(name='prodsys.tasks.get_proxy')
def get_proxy():
    logger = logging.getLogger('periodic_tasks_logger')
    getRotatingFileHandler(logger, 'celery.get_proxy.log')
    proxy_local = '/tmp/x509up_u%s' % os.geteuid()
    
    proxy_user_id = settings.PROXY_USER_ID
    proxy_password = settings.PROXY_PASSWORD
    
    try:
        ctx = saga.Context("UserPass")
        ctx.user_id = proxy_user_id # remote login name
        ctx.user_pass = proxy_password # password
        if os.path.isfile(proxy_local):
            old_proxy = os.stat(proxy_local).st_mtime
            logger.info("Current proxy: %s" % time.ctime(old_proxy)) 

        logger.info('connect to pandawms')
        session = saga.Session()
        session.add_context(ctx)
    
        js = saga.job.Service("ssh://pandawms.jinr.ru", session=session)

        jd = saga.job.Description()        
        jd.executable      = "voms-proxy-init -voms vo.compass.cern.ch:/vo.compass.cern.ch/Role=production --valid 96:00 -q -old --out /home/virthead/x509up_u500 -pwstdin < proxy/gp"
        jd.output          = "/home/virthead/proxy/GetProxy.stdout"  # full path to remote stdout
        jd.error           = "/home/virthead/proxy/GetProxy.stderr"  # full path to remote srderr

        myjob = js.create_job(jd)
        myjob.run()
        myjob.wait()
        old_proxy = 0.0
        outfilesource = 'sftp://pandawms.jinr.ru/home/virthead/x509up_u500'   # path to proxy
        outfiletarget = 'file://localhost%s' % proxy_local
        logger.info('start loading proxy')
        load = True
        while load:
            out = saga.filesystem.File(outfilesource, session=session)
            out.copy(outfiletarget)
            new_proxy = os.stat(proxy_local).st_mtime
            if new_proxy > old_proxy:
                load = False
        logger.info('proxy loaded')
        new_proxy = os.stat(proxy_local).st_mtime
        logger.info("New proxy: %s" % time.ctime(new_proxy))
        return 0
        
    except saga.SagaException, ex:
        # Catch all saga exceptions
        logger.exception("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        logger.exception(" \n*** Backtrace:\n %s" % ex.traceback)
        return -1

    return True

@shared_task
def send_jobs(max_send_amount=10, site='CERN_COMPASS_PROD'):
    logger = logging.getLogger('compass.prodsys.tasks.send_jobs')
    logger.info('i''m in')
    
    return True

@shared_task(name='prodsys.tasks.x_check_mdst')
def x_check_mdst():
    logger = logging.getLogger('periodic_tasks_logger')
    getRotatingFileHandler(logger, 'celery.x_check_mdst.log')
    
    today = datetime.datetime.today()
    logger.info('Starting %s' % __name__)
    
    logger.info('Getting tasks with status send and running')
    tasks_list = Task.objects.all().filter(Q(status='send') | Q(status='running'))
#    tasks_list = Task.objects.all().filter(name='dvcs2016P07t1_mu+_part1')
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    for t in tasks_list:
        logger.info('Getting run numbers for task %s status merging finished and status x check no' % t.name)
        runs_list = Job.objects.filter(task=t).filter(status_merging_mdst='finished').filter(status_x_check='no').order_by('run_number').values_list('run_number', flat=True).distinct()
        logger.info('Got list of %s runs' % len(runs_list))
        if len(runs_list) == 0:
            logger.info('No runs found for checking')
            continue
        
        rnum = 0
        for run_number in runs_list:
    #         if rnum == 1:
    #             break
            
            logger.info('Getting all jobs of run number %s' % run_number)
            jobs_list_count = Job.objects.all().filter(task=t).filter(run_number=run_number).values_list('panda_id_merging_mdst', flat=True).distinct().count()
            logger.info(jobs_list_count)
            
            nEvents_run = 0
            nEvents_chunks = 0
            
            logger.info('Getting jobs with merging mdst status finished for task %s and run number %s' % (t.name, run_number))
            merging_jobs_list = list(Job.objects.filter(task=t).filter(run_number=run_number).filter(status_merging_mdst='finished').values_list('panda_id_merging_mdst', flat=True).distinct())
            logger.info('Got list of %s jobs' % len(merging_jobs_list))
            if len(merging_jobs_list) != jobs_list_count:
                logger.info('Not all jobs of run are ready for checking, skipping')
                continue
                
            logger.info('Sending request to PanDA server')
            s,o = Client.getJobStatus(merging_jobs_list, None)
            if s == 0:
                for x in o:
                    for j in merging_jobs_list:
                        if j == x.PandaID:
                            nEvents_run += x.nEvents
                            
                            logger.info('Got merging job with % events' % x.nEvents)
                            logger.info('Going to get chunks with panda_id_merging_mdst=%s' % x.PandaID)
                            jobs_list = list(Job.objects.filter(panda_id_merging_mdst=x.PandaID).values_list('panda_id', flat=True))
                            logger.info('Got list of %s jobs' % len(jobs_list))
                            logger.info('Sending request to PanDA server')
                            s1,o1 = Client.getJobStatus(jobs_list, None)
                            if s1 == 0:
                                for x1 in o1:
                                    nEvents_chunks += x1.nEvents
                            
            logger.info('Number of events in merged files of run %s and sum of events in chunks: %s - %s' % (run_number, nEvents_run, nEvents_chunks))
            
            if nEvents_run == nEvents_chunks:
                logger.info('Number of events in merged files and run are equal')
                logger.info('Going to update x check status of jobs of run number %s to yes' % run_number)
                jobs_list_update = Job.objects.filter(task=t).filter(run_number=run_number).update(status_x_check='yes', date_updated=today)
            else:
                logger.info('Number of events in merged files and run are not equal')
                logger.info('Going to update x check status of jobs of run number %s to manual check is needed' % run_number)
                jobs_list_update = Job.objects.filter(task=t).filter(run_number=run_number).update(status_x_check='manual check is needed', date_updated=today)
                
            rnum += 1
        
    logger.info('done')
        
    return True