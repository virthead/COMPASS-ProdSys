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
import random

import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

from django.db.models import Q
from prodsys.models import Task, Job
from schedconfig.models import Jobsactive4

from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.send_jobs.log')

today = datetime.datetime.today()
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

def main():
    logger.info('Getting tasks with status send and running')
    tasks_list = Task.objects.all().filter(Q(status='send') | Q(status='running'))
    #tasks_list = Task.objects.all().filter(name='dvcs2017align7_mu-')
    logger.info('Got list of %s tasks' % len(tasks_list))
    
    cdbServerArr = ['compassvm21.cern.ch', 'compassvm22.cern.ch', 'compassvm23.cern.ch']
    cdbServer = cdbServerArr[0]
    
    for t in tasks_list:
        max_send_amount = 1000

        logger.info('Getting jobs in status staged or failed for task %s' % t)
        jobs_list_count = Job.objects.all().filter(task=t).filter(attempt__lt=t.max_attempts).filter(Q(status='staged') | Q(status='failed')).count()
        if jobs_list_count > 50:
            jobs_list = Job.objects.all().filter(task=t).filter(attempt__lt=t.max_attempts).filter(Q(status='staged') | Q(status='failed')).order_by('-number_of_events')[:max_send_amount]
        else:
            jobs_list = Job.objects.all().filter(task=t).filter(attempt__lt=t.max_attempts).filter(Q(status='staged') | Q(status='failed')).order_by('-number_of_events')[:jobs_list_count]
        logger.info('Got list of %s jobs' % len(jobs_list))
        
    #    jobs_list = Job.objects.all().filter(task=t).filter(file='/castor/cern.ch/compass/data/2017/raw/W04/cdr12116-278485.raw')
        
        i = 0
        for j in jobs_list:
            if j.attempt >= j.task.max_attempts:
                logger.info('Number of retry attempts has reached for job %s of task %s' % (j.file, j.task.name))
                continue
            
            if i > max_send_amount:
                break
            
            logger.info('Job %s of %s' % (i, max_send_amount))
            logger.info('Going to send job %s of %s task' % (j.file, j.task.name))
            
            umark = commands.getoutput('uuidgen')
            datasetName = 'panda.destDB.%s' % umark 
            destName    = 'local' # PanDA will not try to move output data, data will be placed by pilot (based on schedconfig)
            TMPRAWFILE = j.file[j.file.rfind('/') + 1:]
            logger.info(TMPRAWFILE)
            TMPMDSTFILE = 'mDST-%(runNumber)s-%(runChunk)s-%(prodSlt)s-%(phastVer)s.root' % {'input_file': j.file, 'runNumber': j.run_number, 'runChunk': j.chunk_number, 'prodSlt': j.task.prodslt, 'phastVer': j.task.phastver}
            logger.info(TMPMDSTFILE)
            TMPHISTFILE = '%(runNumber)s-%(runChunk)s-%(prodSlt)s.root' % {'runNumber': j.run_number, 'runChunk': j.chunk_number, 'prodSlt': j.task.prodslt}
            logger.info(TMPHISTFILE)
            TMPRICHFILE = 'gfile_%(runNumber)s-%(runChunk)s.gfile' % {'runNumber': j.run_number, 'runChunk': j.chunk_number}
            logger.info(TMPRICHFILE)
            EVTDUMPFILE = 'evtdump%(prodSlt)s-%(runChunk)s-%(runNumber)s.raw' % {'prodSlt': j.task.prodslt, 'runNumber': j.run_number, 'runChunk': j.chunk_number}
            logger.info(EVTDUMPFILE)
            STDOUTFILE = '%(prodNameOnly)s.%(runNumber)s-%(runChunk)s-%(prodSlt)s.stdout' % {'prodNameOnly': j.task.production, 'runNumber': j.run_number, 'runChunk': j.chunk_number, 'prodSlt': j.task.prodslt}
            logger.info(STDOUTFILE)
            STDERRFILE = '%(prodNameOnly)s.%(runNumber)s-%(runChunk)s-%(prodSlt)s.stderr' % {'prodNameOnly': j.task.production, 'runNumber': j.run_number, 'runChunk': j.chunk_number, 'prodSlt': j.task.prodslt}
            logger.info(STDERRFILE)
            PRODSOFT = j.task.soft
            logger.info(PRODSOFT)
            try:
                file_year = j.file.split('/')[5]
                logger.info(file_year)
            except:
                logger.error('Error while splitting file to get year')
                sys.exit(1)
                
            ProdPathAndName = j.task.home + j.task.path + j.task.soft
        
            job = JobSpec()
            job.taskID = j.task.id
            job.jobDefinitionID   = 0
            job.jobName           = '%(prodName)s-%(fileYear)s--%(runNumber)s-%(runChunk)s-%(prodSlt)s-%(phastVer)s' % {'prodName': j.task.production, 'fileYear': file_year, 'runNumber': j.run_number, 'runChunk': j.chunk_number, 'prodSlt': j.task.prodslt, 'phastVer': j.task.phastver}
            job.transformation    = j.task.type # payload (can be URL as well)
            job.destinationDBlock = datasetName
            job.destinationSE     = destName
            job.currentPriority   = 2000
            if j.task.type == 'DDD filtering':
                job.currentPriority = 1000
            job.prodSourceLabel   = 'prod_test'
            job.computingSite     = j.task.site
            job.attemptNr = j.attempt + 1
            job.maxAttempt = j.task.max_attempts
            if j.status == 'failed':
                job.parentID = j.panda_id
            head, tail = os.path.split(j.file)
            
            cdbServer = cdbServerArr[random.randrange(0,2)]
            
            # logs, and all files generated during execution will be placed in log (except output file)
            #job.jobParameters='source /afs/cern.ch/project/eos/installation/compass/etc/setup.sh;export EOS_MGM_URL=root://eoscompass.cern.ch;export PATH=/afs/cern.ch/project/eos/installation/compass/bin:$PATH;ppwd=$(pwd);echo $ppwd;export TMPMDSTFILE=%(TMPMDSTFILE)s;export TMPHISTFILE=%(TMPHISTFILE)s;export TMPRICHFILE=%(TMPRICHFILE)s;coralpath=%(ProdPathAndName)s/coral;echo $coralpath;cd -P $coralpath;export coralpathsetup=$coralpath"/setup.sh";echo $coralpathsetup;source $coralpathsetup;cd $ppwd;$CORAL/../phast/coral/coral.exe %(ProdPathAndName)s/template.opt;xrdcp -np $ppwd/%(TMPMDSTFILE)s xroot://eoscompass.cern.ch//eos/compass/%(prodName)s/mDST/%(TMPMDSTFILE)s;xrdcp -np $ppwd/%(TMPHISTFILE)s xroot://eoscompass.cern.ch//eos/compass/%(prodName)s/histos/%(TMPHISTFILE)s;metadataxml=$(ls metadata-*);echo $metadataxml;cp $metadataxml $metadataxml.PAYLOAD;' % {'TMPMDSTFILE': TMPMDSTFILE, 'TMPHISTFILE': TMPHISTFILE, 'TMPRICHFILE': TMPRICHFILE, 'input_file': input_file, 'ProdPathAndName': ProdPathAndName, 'prodName': prodName}
            if j.task.type == 'test production' or j.task.type == 'mass production' or j.task.type == 'technical production':
                if j.task.site == 'BW_COMPASS_MCORE':
                    job.jobParameters='ppwd=$(pwd);export COMPASS_SW_PREFIX=/scratch/sciteam/criedl/projectdata/;export COMPASS_SW_PATH=%(prodPath)s;export COMPASS_PROD_NAME=%(prodName)s;export TMPRAWFILE=%(TMPRAWFILE)s;export TMPMDSTFILE=%(TMPMDSTFILE)s;export TMPHISTFILE=%(TMPHISTFILE)s;export TMPRICHFILE=%(TMPRICHFILE)s;export prodSlt=%(prodSlt)s;export EVTDUMPFILE=%(EVTDUMPFILE)s;export PRODSOFT=%(PRODSOFT)s;cp %(input_file)s .;coralpath=%(ProdPathAndName)s/coral;cd -P $coralpath;export coralpathsetup=$coralpath"/setup.sh";source $coralpathsetup;cd $ppwd;$CORAL/../phast/coral/coral.exe %(ProdPathAndName)s/%(template)s;if [ ! -s testevtdump.raw ]; then echo "PanDA message: the file is empty">testevtdump.raw; fi;cp payload_stderr.txt payload_stderr.out;cp payload_stdout.txt payload_stdout.out;rm %(tail)s' % {'TMPRAWFILE': TMPRAWFILE, 'TMPMDSTFILE': TMPMDSTFILE, 'TMPHISTFILE': TMPHISTFILE, 'TMPRICHFILE': TMPRICHFILE, 'PRODSOFT': PRODSOFT, 'input_file': j.file, 'ProdPathAndName': ProdPathAndName, 'prodPath': j.task.path, 'prodName': j.task.production, 'template': j.task.template, 'tail': tail, 'prodSlt': j.task.prodslt, 'EVTDUMPFILE': EVTDUMPFILE, 'STDOUTFILE': STDOUTFILE, 'STDERRFILE': STDERRFILE}
                else:
                    job.jobParameters='export EOS_MGM_URL=root://eoscompass.cern.ch;ppwd=$(pwd);export COMPASS_SW_PREFIX=/eos/experiment/compass/;export COMPASS_SW_PATH=%(prodPath)s;export COMPASS_PROD_NAME=%(prodName)s;export TMPRAWFILE=%(TMPRAWFILE)s;export TMPMDSTFILE=%(TMPMDSTFILE)s;export TMPHISTFILE=%(TMPHISTFILE)s;export TMPRICHFILE=%(TMPRICHFILE)s;export prodSlt=%(prodSlt)s;export EVTDUMPFILE=%(EVTDUMPFILE)s;export PRODSOFT=%(PRODSOFT)s;xrdcp -N -f root://castorpublic.cern.ch/%(input_file)s\?svcClass=compasscdr .;coralpath=%(ProdPathAndName)s/coral;cd -P $coralpath;export coralpathsetup=$coralpath"/setup.sh";source $coralpathsetup;cd $ppwd;export CDBSERVER=%(cdbServer)s;$CORAL/../phast/coral/coral.exe %(ProdPathAndName)s/%(template)s;if [ ! -s testevtdump.raw ]; then echo "PanDA message: the file is empty">testevtdump.raw; fi;cp payload_stderr.txt payload_stderr.out;cp payload_stdout.txt payload_stdout.out;rm %(tail)s' % {'TMPRAWFILE': TMPRAWFILE, 'TMPMDSTFILE': TMPMDSTFILE, 'TMPHISTFILE': TMPHISTFILE, 'TMPRICHFILE': TMPRICHFILE, 'PRODSOFT': PRODSOFT, 'input_file': j.file, 'ProdPathAndName': ProdPathAndName, 'prodPath': j.task.path, 'prodName': j.task.production, 'template': j.task.template, 'tail': tail, 'prodSlt': j.task.prodslt, 'EVTDUMPFILE': EVTDUMPFILE, 'STDOUTFILE': STDOUTFILE, 'STDERRFILE': STDERRFILE, 'cdbServer': cdbServer}
            if j.task.type == 'DDD filtering':
                job.jobParameters='export EOS_MGM_URL=root://eoscompass.cern.ch;ppwd=$(pwd);export COMPASS_SW_PREFIX=/eos/experiment/compass/;export COMPASS_SW_PATH=%(prodPath)s;export COMPASS_PROD_NAME=%(prodName)s;export TMPRAWFILE=%(TMPRAWFILE)s;export TMPMDSTFILE=%(TMPMDSTFILE)s;export TMPHISTFILE=%(TMPHISTFILE)s;export TMPRICHFILE=%(TMPRICHFILE)s;export prodSlt=%(prodSlt)s;export EVTDUMPFILE=%(EVTDUMPFILE)s;export PRODSOFT=%(PRODSOFT)s;xrdcp -N -f root://castorpublic.cern.ch/%(input_file)s\?svcClass=compasscdr .;coralpath=%(ProdPathAndName)s/coral;cd -P $coralpath;export coralpathsetup=$coralpath"/setup.sh";source $coralpathsetup;cd $ppwd;$CORAL/src/DaqDataDecoding/examples/how-to/ddd --filter-CAL --out=testevtdump.raw %(TMPRAWFILE)s;if [ ! -s testevtdump.raw ]; then echo "PanDA message: the file is empty">testevtdump.raw; fi;cp payload_stderr.txt payload_stderr.out;cp payload_stdout.txt payload_stdout.out;rm %(tail)s' % {'TMPRAWFILE': TMPRAWFILE, 'TMPMDSTFILE': TMPMDSTFILE, 'TMPHISTFILE': TMPHISTFILE, 'TMPRICHFILE': TMPRICHFILE, 'PRODSOFT': PRODSOFT, 'input_file': j.file, 'ProdPathAndName': ProdPathAndName, 'prodPath': j.task.path, 'prodName': j.task.production, 'template': j.task.template, 'tail': tail, 'prodSlt': j.task.prodslt, 'EVTDUMPFILE': EVTDUMPFILE, 'STDOUTFILE': STDOUTFILE, 'STDERRFILE': STDERRFILE}
    
    #     fileIRaw = FileSpec()
    #     fileIRaw.lfn = "%s" % (input_file)
    #     fileIRaw.destinationDBlock = job.destinationDBlock
    #     fileIRaw.destinationSE     = job.destinationSE
    #     fileIRaw.dataset           = job.destinationDBlock
    #     fileIRaw.type = 'input'
    #     job.addFile(fileIRaw)
        
            fileOstdout = FileSpec()
            fileOstdout.lfn = "payload_stdout.txt"
            fileOstdout.destinationDBlock = job.destinationDBlock
            fileOstdout.destinationSE     = job.destinationSE
            fileOstdout.dataset           = job.destinationDBlock
            fileOstdout.type = 'output'
            job.addFile(fileOstdout)
        
            fileOstderr = FileSpec()
            fileOstderr.lfn = "payload_stderr.txt"
            fileOstderr.destinationDBlock = job.destinationDBlock
            fileOstderr.destinationSE     = job.destinationSE
            fileOstderr.dataset           = job.destinationDBlock
            fileOstderr.type = 'output'
            job.addFile(fileOstderr)
        
            fileOLog = FileSpec()
            fileOLog.lfn = "%(prodName)s-%(runNumber)s-%(runChunk)s-%(prodSlt)s-%(phastVer)s.job.log.tgz" % {'prodName': j.task.production, 'runNumber': j.run_number, 'runChunk': j.chunk_number, 'prodSlt': j.task.prodslt, 'phastVer': j.task.phastver}
            fileOLog.destinationDBlock = job.destinationDBlock
            fileOLog.destinationSE     = job.destinationSE
            fileOLog.dataset           = job.destinationDBlock
            fileOLog.type = 'log'
            job.addFile(fileOLog)
    
            if j.task.type == 'test production' or j.task.type == 'mass production' or j.task.type == 'technical production':
                fileOmDST = FileSpec()
                fileOmDST.lfn = "%s" % (TMPMDSTFILE)
                fileOmDST.destinationDBlock = job.destinationDBlock
                fileOmDST.destinationSE     = job.destinationSE
                fileOmDST.dataset           = job.destinationDBlock
                fileOmDST.type = 'output'
                job.addFile(fileOmDST)
                
                fileOTrafdic = FileSpec()
                fileOTrafdic.lfn = "%s" % (TMPHISTFILE)
                fileOTrafdic.destinationDBlock = job.destinationDBlock
                fileOTrafdic.destinationSE     = job.destinationSE
                fileOTrafdic.dataset           = job.destinationDBlock
                fileOTrafdic.type = 'output'
                job.addFile(fileOTrafdic)
        
            if j.task.type == 'test production' or j.task.type == 'mass production' or j.task.type == 'technical production' or j.task.type == 'DDD filtering':
                fileOtestevtdump = FileSpec()
                fileOtestevtdump.lfn = "testevtdump.raw"
                fileOtestevtdump.destinationDBlock = job.destinationDBlock
                fileOtestevtdump.destinationSE     = job.destinationSE
                fileOtestevtdump.dataset           = job.destinationDBlock
                fileOtestevtdump.type = 'output'
                job.addFile(fileOtestevtdump)
        
            s,o = Client.submitJobs([job],srvID=aSrvID)
            logger.info(s)
            for x in o:
                logger.info("PandaID=%s" % x[0])
                today = datetime.datetime.today()
                
                if x[0] != 0 and x[0] != 'NULL':
                    j_update = Job.objects.get(id=j.id)
                    j_update.panda_id = x[0]
                    j_update.status = 'sent'
                    j_update.attempt = j_update.attempt + 1
                    j_update.date_updated = today
                
                    try:
                        j_update.save()
                        logger.info('Job %s with PandaID %s updated at %s' % (j.id, x[0], today)) 
                    except IntegrityError as e:
                        logger.exception('Unique together catched, was not saved')
                    except DatabaseError as e:
                        logger.exception('Something went wrong while saving: %s' % e.message)
                else:
                    logger.info('Job %s was not added to PanDA' % j.id)
            i += 1
    
    logger.info('done')

if __name__ == "__main__":
    sys.exit(main())
