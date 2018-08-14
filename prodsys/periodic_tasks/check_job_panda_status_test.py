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

jobs_list = list(Job.objects.filter(panda_id=823637).values_list('panda_id', flat=True))
print 'Got list of %s jobs' % len(jobs_list)
print 'Sending request to PanDA server'
s,o = Client.getJobStatus(jobs_list, None)
if s == 0:
        for x in o:
            print 'Getting job for PandaID=%s' % x.PandaID
            j_update = Job.objects.get(panda_id=x.PandaID)
            print x.jobStatus
            print x.pilotErrorCode
            print x.pilotErrorDiag
            if x.pilotErrorCode == 1234 or x.pilotErrorCode == 1235 or x.pilotErrorCode == 1236:  
                print 'manual check is needed'
            else:
                print x.jobStatus
            
print 'done'
