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

print 'Getting tasks with status send and running'
tasks_list = Task.objects.all().filter(Q(status='send') | Q(status='running'))
print 'Got list of %s tasks' % len(tasks_list)

for t in tasks_list:
    print 'Getting jobs with status send and running for task with status send and running'
    jobs_list = list(Job.objects.filter(task=t).filter(Q(status='sent') | Q(status='running')).values_list('panda_id', flat=True)[:50])
    print 'Got list of %s jobs' % len(jobs_list)
    print 'Sending request to PanDA server'
    s,o = Client.getJobStatus(jobs_list, None)
    if s == 0:
        for x in o:
            print 'Getting job for PandaID=%s' % x.PandaID
            j_update = Job.objects.get(panda_id=x.PandaID)
            if j_update.status != x.jobStatus:
                    today = datetime.datetime.today()

                    if x.jobStatus == 'running' or x.jobStatus == 'finished' or x.jobStatus == 'failed':
                        print 'Going to update status of job %s from %s to %s' % (j_update.file, j_update.status, x.jobStatus)
                        j_update.status = x.jobStatus
                        j_update.date_updated = today
                        if x.jobStatus == 'finished':
                            j_update.status_merging_mdst = 'ready'
                    
                        try:
                            j_update.save()
                            print 'Job %s with PandaID %s updated' % (j_update.id, j_update.panda_id) 
                        except IntegrityError as e:
                            print 'Unique together catched, was not saved'
                        except DatabaseError as e:
                            print 'Something went wrong while saving: %s' % e.message
                    
                    if x.jobStatus == 'running' and j_update.task.status == 'send':
                        print 'Going to update status of task %s from send to running' % j_update.task.name
                        t_update = Task.objects.get(id=j_update.task.id)
                        t_update.status = 'running'
                        t_update.date_updated = today
                    
                        try:
                            t_update.save()
                            print 'Task %s updated' % t_update.name 
                        except IntegrityError as e:
                            print 'Unique together catched, was not saved'
                        except DatabaseError as e:
                            print 'Something went wrong while saving: %s' % e.message
                    
print 'done'
