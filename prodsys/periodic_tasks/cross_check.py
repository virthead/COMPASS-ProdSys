#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import commands
import datetime
from django.conf import settings
from django.core.wsgi import get_wsgi_application
from django.db import DatabaseError, IntegrityError
from _mysql import NULL

today = datetime.datetime.today()
print today
print 'Starting %s' % __file__

import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")

print 'Setting environment for PanDA client'
aSrvID = None
site = 'CERN_COMPASS_PROD'
os.environ["PANDA_URL_SSL"] = settings.PANDA_URL_SSL
os.environ["PANDA_URL"] = settings.PANDA_URL
os.environ["X509_USER_PROXY"] = settings.X509_USER_PROXY

print 'PanDA URL SSL: %s' % os.environ["PANDA_URL_SSL"]

application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job

print 'Getting tasks with status send|running|done'
tasks_list = Task.objects.all().filter(Q(status='send') | Q(status='running') | Q(status='done')).order_by('id')
for t in tasks_list:
    merge_tail = 0
    
    print 'Getting jobs for task %s with status finished and status merging finished' % t.name
    merging_jobs_list = Job.objects.filter(task=t).filter(status='finished').filter(status_merging_mdst='finished').filter(status_x_check='no').order_by('panda_id_merging_mdst').values_list('panda_id_merging_mdst', flat=True).distinct()
    print 'Got list of %s jobs' % len(merging_jobs_list)
    if len(merging_jobs_list) == 0:
        print 'No merging jobs found for cross-checking'
        continue
    
    for j in merging_jobs_list:
        nEvents = 0
        print 'Getting number of events of merging job %s' % j
        s,o = Client.getJobStatus([j], None)
        if s == 0:
            for x in o:
                nEvents = x.nEvents
        
        print 'nEvents = %s' % nEvents        
        print 'Getting all jobs with panda_id_merging_mdst %s' % j
        jobs_list = Job.objects.all().filter(panda_id_merging_mdst=j).filter(status='finished').order_by('id')
        if len(jobs_list) == 0:
            print 'No jobs found for cross-checking'
            continue
        
        nEventsSum = 0
        print 'Getting sum of number of events of jobs'
        for k in jobs_list:
            s,o = Client.getJobStatus([k.panda_id], None)
            if s == 0:
                for x in o:
                    nEventsSum += x.nEvents
        
        print 'nEventsSum = %s' % nEventsSum
        if nEvents == nEventsSum:
            print 'Going to update all jobs to status_x_check x-checked'
            for k in jobs_list:
                j_update = Job.objects.get(id=k.id)
                j_update.status = x.jobStatus
                j_update.status_x_check = 'yes'
                    
                try:
                    j_update.save()
                    print 'Job %s with PandaID %s updated' % (k.id, k.panda_id) 
                except IntegrityError as e:
                    print 'Unique together catched, was not saved'
                except DatabaseError as e:
                    print 'Something went wrong while saving: %s' % e.message
        else:
            print 'Going to update all merging jobs to status_x_check failed'
            for k in jobs_list:
                j_update = Job.objects.get(id=k.id)
                j_update.status = x.jobStatus
                j_update.status_x_check = 'failed'
                    
                try:
                    j_update.save()
                    print 'Job %s with PandaID %s updated' % (k.id, k.panda_id) 
                except IntegrityError as e:
                    print 'Unique together catched, was not saved'
                except DatabaseError as e:
                    print 'Something went wrong while saving: %s' % e.message
            
print 'done'
