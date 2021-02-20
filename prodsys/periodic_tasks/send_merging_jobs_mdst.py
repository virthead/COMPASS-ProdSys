#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
import commands
from django.utils import timezone
from django.conf import settings
import logging
from django.core.wsgi import get_wsgi_application
from django.db import DatabaseError, IntegrityError
from _mysql import NULL

merging_size = 4500000000

import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job
from schedconfig.models import Filestable4

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.send_merging_jobs_mdst.log')

logger.info('Starting %s' % __file__)

logger.info('Setting environment for PanDA client')
aSrvID = None
os.environ["PANDA_URL_SSL"] = settings.PANDA_URL_SSL
os.environ["PANDA_URL"] = settings.PANDA_URL
os.environ["X509_USER_PROXY"] = settings.X509_USER_PROXY

logger.info('PanDA URL SSL: %s' % os.environ["PANDA_URL_SSL"])

pid = str(os.getpid())
logger.info('pid: %s' % pid)
if check_process(__file__, pid):
    logger.info('Another %s process is running, exiting' % __file__)
    sys.exit(0)

def send_merging_job(task, files_list, merge_chunk_number):
    logger.info('Going to send merging job for task %s run number %s and merge chunk number %s' % (task, files_list[0].run_number, merge_chunk_number)) 
    
    input_files = ''
    input_files_copy = ''
    mc = ''
    if task.type == 'MC reconstruction':
        mc = 'mc/'
    
    for j in files_list:
        TMPMDSTFILE = 'mDST-%(runNumber)s-%(runChunk)s-%(prodSlt)s-%(phastVer)s.root' % {'runNumber': j.run_number, 'runChunk': j.chunk_number, 'prodSlt': j.task.prodslt, 'phastVer': j.task.phastver}
        input_files += ' ' + TMPMDSTFILE
        if j.task.site == 'BW_COMPASS_MCORE' or j.task.site == 'STAMPEDE_COMPASS_MCORE' or j.task.site == 'FRONTERA_COMPASS_MCORE':
            input_files_copy += ' cp ' + task.files_home_prefix + task.path + task.soft + '/mDST.chunks/' + TMPMDSTFILE + ' .;'
        else:
            input_files_copy += ' xrdcp -N -f ' + settings.EOS_HOME_ROOT + settings.EOS_HOME + mc + task.path + task.soft + '/mDST.chunks/' + TMPMDSTFILE + ' .;'
    
    datasetName = '%(prodNameOnly)s.%(runNumber)s-%(prodSlt)s-%(phastVer)s-merging-mdst' % {'prodNameOnly': task.production, 'runNumber': j.run_number, 'prodSlt': task.prodslt, 'phastVer': task.phastver}
    logger.info(datasetName)
    destName    = 'local' # PanDA will not try to move output data, data will be placed by pilot (based on schedconfig)
    MERGEDHISTFILE = '%(runNumber)s-%(prodSlt)s-%(phastVer)s.root' % {'runNumber': j.run_number, 'prodSlt': task.prodslt, 'phastVer': task.phastver}
    if format(merge_chunk_number, '03d') != '000':
        MERGEDHISTFILE = MERGEDHISTFILE + '.' + format(merge_chunk_number, '03d')
    logger.info(MERGEDHISTFILE)
    MERGEDMDSTFILE = 'mDST-%(runNumber)s-%(prodSlt)s-%(phastVer)s.root' % {'runNumber': j.run_number, 'prodSlt': task.prodslt, 'phastVer': task.phastver}
    if format(merge_chunk_number, '03d') != '000':
        MERGEDMDSTFILE = MERGEDMDSTFILE + '.' + format(merge_chunk_number, '03d')
    logger.info(MERGEDMDSTFILE)
    TMPHISTFILE = 'merge-%(runNumber)s-ch%(mergeChunkNumber)s.root' % {'runNumber': j.run_number, 'mergeChunkNumber': format(merge_chunk_number, '03d')}
    logger.info(TMPHISTFILE)
    PRODSOFT = task.soft
    ProdPathAndName = task.home + task.path + task.soft
    
    job = JobSpec()
    job.VO = 'vo.compass.cern.ch'
    job.taskID = task.id
    job.jobDefinitionID   = 0
    job.jobName           = '%(prodNameOnly)s-merge-mdst-%(runNumber)s-ch%(mergeChunkNumber)s' % {'prodNameOnly': task.production, 'runNumber': j.run_number, 'mergeChunkNumber': format(merge_chunk_number, '03d')}
    job.transformation    = 'merging mdst' # payload (can be URL as well)
    job.destinationDBlock = datasetName
    job.destinationSE     = destName
    job.currentPriority   = 5000
    job.prodSourceLabel   = 'prod_test'
    if j.task.site == 'BW_COMPASS_MCORE' or j.task.site == 'STAMPEDE_COMPASS_MCORE' or j.task.site == 'FRONTERA_COMPASS_MCORE':
        job.computingSite     = task.site + '_MERGING'
    else:
        job.computingSite     = task.site
    job.attemptNr = j.attempt_merging_mdst + 1
    job.maxAttempt = j.task.max_attempts
    if j.status_merging_mdst == 'failed':
        job.parentID = j.panda_id_merging_mdst
    
    if j.task.site == 'BW_COMPASS_MCORE' or j.task.site == 'STAMPEDE_COMPASS_MCORE' or j.task.site == 'FRONTERA_COMPASS_MCORE':
        job.jobParameters='ppwd=$(pwd);ppwd=$(pwd);export COMPASS_SW_PREFIX=%(filesHomePrefix)s;export COMPASS_SW_PATH=%(prodPath)s;export COMPASS_PROD_NAME=%(prodName)s;export prodSlt=%(prodSlt)s;export MERGEDHISTFILE=%(MERGEDHISTFILE)s;export MERGEDMDSTFILE=%(MERGEDMDSTFILE)s;export TMPHISTFILE=%(TMPHISTFILE)s;export PRODSOFT=%(PRODSOFT)s;coralpath=%(ProdPathAndName)s/coral;cd -P $coralpath;export coralpathsetup=$coralpath"/setup.sh";source $coralpathsetup;cd $ppwd;%(input_files_copy)sexport PHAST_mDST_MAX_SIZE=6000000000;$CORAL/../../phast-production/phast -m -o %(MERGEDMDSTFILE)s %(input_files)s;rm %(input_files)s;cp payload_stderr.txt payload_stderr.out;cp payload_stdout.txt payload_stdout.out;gzip payload_stdout.out;' % {'filesHomePrefix': j.task.files_home_prefix, 'MERGEDHISTFILE': MERGEDHISTFILE, 'MERGEDMDSTFILE': MERGEDMDSTFILE, 'PRODSOFT': PRODSOFT, 'input_files_copy': input_files_copy, 'input_files': input_files, 'ProdPathAndName': ProdPathAndName, 'prodPath': task.path, 'prodName': task.production, 'prodSlt': task.prodslt, 'TMPHISTFILE': TMPHISTFILE}
    else:
        if j.task.type == 'MC reconstruction':
            job.jobParameters='export TASK_TYPE=MCRECO;export EOS_MGM_URL=%(eosHomeRoot)s;ppwd=$(pwd);export COMPASS_SW_PREFIX=%(eosHome)s;export COMPASS_SW_PATH=%(prodPath)s;export COMPASS_PROD_NAME=%(prodName)s;export prodSlt=%(prodSlt)s;export MERGEDHISTFILE=%(MERGEDHISTFILE)s;export MERGEDMDSTFILE=%(MERGEDMDSTFILE)s;export TMPHISTFILE=%(TMPHISTFILE)s;export PRODSOFT=%(PRODSOFT)s;coralpath=%(ProdPathAndName)s;cd -P $coralpath;export coralpathsetup=$coralpath"/environment.sh";source $coralpathsetup;cd $ppwd;%(input_files_copy)sexport PHAST_mDST_MAX_SIZE=6000000000;$CORAL/bin/phast -m -o %(MERGEDMDSTFILE)s %(input_files)s;cp payload_stderr.txt payload_stderr.out;cp payload_stdout.txt payload_stdout.out;gzip payload_stdout.out;' % {'MERGEDHISTFILE': MERGEDHISTFILE, 'MERGEDMDSTFILE': MERGEDMDSTFILE, 'PRODSOFT': PRODSOFT, 'input_files_copy': input_files_copy, 'input_files': input_files, 'ProdPathAndName': ProdPathAndName, 'prodPath': task.path, 'prodName': task.production, 'prodSlt': task.prodslt, 'TMPHISTFILE': TMPHISTFILE, 'eosHomeRoot':settings.EOS_HOME_ROOT, 'eosHome': settings.EOS_HOME}
        else:
            job.jobParameters='export EOS_MGM_URL=%(eosHomeRoot)s;ppwd=$(pwd);export COMPASS_SW_PREFIX=%(eosHome)s;export COMPASS_SW_PATH=%(prodPath)s;export COMPASS_PROD_NAME=%(prodName)s;export prodSlt=%(prodSlt)s;export MERGEDHISTFILE=%(MERGEDHISTFILE)s;export MERGEDMDSTFILE=%(MERGEDMDSTFILE)s;export TMPHISTFILE=%(TMPHISTFILE)s;export PRODSOFT=%(PRODSOFT)s;coralpath=%(ProdPathAndName)s/coral;cd -P $coralpath;export coralpathsetup=$coralpath"/setup.sh";source $coralpathsetup;cd $ppwd;%(input_files_copy)sexport PHAST_mDST_MAX_SIZE=6000000000;$CORAL/../phast/phast -m -o %(MERGEDMDSTFILE)s %(input_files)s;cp payload_stderr.txt payload_stderr.out;cp payload_stdout.txt payload_stdout.out;gzip payload_stdout.out;' % {'MERGEDHISTFILE': MERGEDHISTFILE, 'MERGEDMDSTFILE': MERGEDMDSTFILE, 'PRODSOFT': PRODSOFT, 'input_files_copy': input_files_copy, 'input_files': input_files, 'ProdPathAndName': ProdPathAndName, 'prodPath': task.path, 'prodName': task.production, 'prodSlt': task.prodslt, 'TMPHISTFILE': TMPHISTFILE, 'eosHomeRoot':settings.EOS_HOME_ROOT, 'eosHome': settings.EOS_HOME}

    fileOLog = FileSpec()
    fileOLog.lfn = "%s.job.log.tgz" % (job.jobName)
    fileOLog.destinationDBlock = job.destinationDBlock
    fileOLog.destinationSE     = job.destinationSE
    fileOLog.dataset           = job.destinationDBlock
    fileOLog.type = 'log'
    job.addFile(fileOLog)
    
    fileOmDST = FileSpec()
    fileOmDST.lfn = "%s" % (MERGEDMDSTFILE)
    fileOmDST.destinationDBlock = job.destinationDBlock
    fileOmDST.destinationSE     = job.destinationSE
    fileOmDST.dataset           = job.destinationDBlock
    fileOmDST.type = 'output'
    job.addFile(fileOmDST)
    
    fileOstdout = FileSpec()
    fileOstdout.lfn = "payload_stdout.out.gz"
    fileOstdout.destinationDBlock = job.destinationDBlock
    fileOstdout.destinationSE     = job.destinationSE
    fileOstdout.dataset           = job.destinationDBlock
    fileOstdout.type = 'output'
    job.addFile(fileOstdout)
    
#     fileOstderr = FileSpec()
#     fileOstderr.lfn = "payload_stderr.txt"
#     fileOstderr.destinationDBlock = job.destinationDBlock
#     fileOstderr.destinationSE     = job.destinationSE
#     fileOstderr.dataset           = job.destinationDBlock
#     fileOstderr.type = 'output'
#     job.addFile(fileOstderr)
    
    s,o = Client.submitJobs([job],srvID=aSrvID)
    logger.info(s)
    for x in o:
        logger.info("PandaID=%s" % x[0])
        if x[0] != 0 and x[0] != 'NULL':
            for j in files_list:
                j_update = Job.objects.get(id=j.id)
                j_update.panda_id_merging_mdst = x[0]
                j_update.status_merging_mdst = 'sent'
                j_update.attempt_merging_mdst = j_update.attempt_merging_mdst + 1
                j_update.chunk_number_merging_mdst = merge_chunk_number
                j_update.date_updated = timezone.now()
              
                try:
                    j_update.save()
                    logger.info('Job %s with PandaID %s updated' % (j.id, x[0])) 
                except IntegrityError as e:
                    logger.exception('Unique together catched, was not saved')
                except DatabaseError as e:
                    logger.exception('Something went wrong while saving: %s' % e.message)
        else:
            logger.info('Job %s was not added to PanDA' % j.id)

def main():    
    logger.info('Getting tasks with status send|running')
    tasks_list = Task.objects.all().exclude(type='DDD filtering').filter(Q(status='send') | Q(status='running'))
    #tasks_list = Task.objects.all().filter(name='dvcs2016P07t1_mu-_part1')
    for t in tasks_list:
        logger.info('Getting jobs for task %s jobs with status finished and status merging failed' % t.name)
        jobs_list_flat = Job.objects.filter(task=t).filter(status='finished').filter(attempt_merging_mdst__lt=t.max_attempts).filter(status_merging_mdst='failed').values_list('panda_id_merging_mdst', 'chunk_number_merging_mdst').distinct()
        logger.info('Got list of %s failed merging jobs' % len(jobs_list_flat))
        for f in jobs_list_flat:
            jobs_list = Job.objects.filter(panda_id_merging_mdst=f[0])
            merging_list = []
            for j in jobs_list:
                merging_list.append(j)
            logger.info('Do send here, clear the list')
            send_merging_job(t, merging_list, f[1])
        
        logger.info('Getting run numbers for task %s with status finished and status merging ready' % t.name)
        runs_list = Job.objects.filter(task=t).filter(status='finished').filter(attempt_merging_mdst__lt=t.max_attempts).filter(status_merging_mdst='ready').order_by('run_number').values_list('run_number', flat=True).distinct()
        logger.info('Got list of %s runs' % len(runs_list))
        if len(runs_list) == 0:
            logger.info('No runs found for merging')
            continue
        
        rnum = 0
        for run_number in runs_list:
    #         if rnum == 1:
    #             break
            
            logger.info('Going to define merging job for run number %s' % run_number)
            
            logger.info('Getting all jobs of run number %s' % run_number)
            jobs_list_count = Job.objects.all().filter(task=t).filter(run_number=run_number).count()
            logger.info(jobs_list_count)
            
            logger.info('Getting jobs for task %s and run number %s with status finished and status merging ready' % (t.name, run_number))
            jobs_list = Job.objects.all().filter(task=t).filter(run_number=run_number).filter(status='finished').filter(status_merging_mdst='ready')
            if len(jobs_list) != jobs_list_count:
                logger.info('Not all jobs of run are ready for merging, skipping')
                continue
            
            merge_chunk_number = 0
            i = 0
            files_size = 0
            merging_list = []
            for j in jobs_list:
                f_check_lfn = 'mDST-%(runNumber)s-%(chunkNumber)s-%(prodSlt)s-%(phastVer)s.root' % {'runNumber': j.run_number, 'chunkNumber': j.chunk_number, 'prodSlt': t.prodslt, 'phastVer': t.phastver}
                logger.info(f_check_lfn)
                f_check = Filestable4.objects.using('schedconfig').get(pandaid=j.panda_id, type='output', lfn=f_check_lfn)
                logger.info(f_check.fsize)
                if files_size + f_check.fsize > merging_size:
                    logger.info(files_size)
                    logger.info('Do send here, clear the list')
                    logger.info(merge_chunk_number)
                    logger.info(len(merging_list))
                    send_merging_job(t, merging_list, merge_chunk_number)
                    files_size = 0
                    merging_list = []
                    
                    files_size = files_size + f_check.fsize
                    merging_list.append(j)
                    merge_chunk_number += 1
                else:
                    files_size = files_size + f_check.fsize
                    merging_list.append(j)
                    logger.info('%s: %s' % (merging_size, files_size))
                
                i += 1
            
            if len(merging_list) > 0:
                logger.info('Merging the tail')
                logger.info(files_size)
                logger.info('Do send here, clear the list')
                logger.info(merge_chunk_number)
                logger.info(len(merging_list))
                send_merging_job(t, merging_list, merge_chunk_number)
            
            rnum += 1
            
    logger.info('done')

if __name__ == "__main__":
    sys.exit(main())
