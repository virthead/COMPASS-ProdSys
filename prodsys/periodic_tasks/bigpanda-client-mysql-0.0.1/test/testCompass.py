import sys
import time
import commands
from userinterface import Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

site = sys.argv[1]

datasetName = 'panda.destDB.%s' % commands.getoutput('uuidgen')
destName    = None

job = JobSpec()
job.jobDefinitionID   = int(time.time()) % 10000
job.jobName           = "%s" % commands.getoutput('uuidgen')
job.transformation    = " " #'-c "";/afs/cern.ch/user/t/tmaeno/public/comtest/generalsh.sh'
job.destinationDBlock = datasetName
job.destinationSE     = destName
job.currentPriority   = 1000
job.prodSourceLabel   = 'test'
job.computingSite     = site

job.jobParameters="pjobrunner.py --taskdict=123_456_67"

fileOL = FileSpec()
fileOL.lfn = "%s.job.log.tgz" % job.jobName
fileOL.destinationDBlock = job.destinationDBlock
fileOL.destinationSE     = job.destinationSE
fileOL.dataset           = job.destinationDBlock
fileOL.type = 'log'
job.addFile(fileOL)


s,o = Client.submitJobs([job])
print s
for x in o:
    print "PandaID=%s" % x[0]

