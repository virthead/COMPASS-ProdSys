import sys
import time
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

aSrvID = None

for idx,argv in enumerate(sys.argv):
    if argv == '-s':
        aSrvID = sys.argv[idx+1]
        sys.argv = sys.argv[:idx]
        break

#site = sys.argv[1]
#site = 'ANALY_BNL-LSST'  #orig
#site = 'BNL-LSST'
#site = 'SWT2_CPB-LSST'
#site = 'UTA_SWT2-LSST'
#site = 'ANALY_SWT2_CPB-LSST'
site = 'ANALY_JINR-LCG2_COMPASS'

datasetName = 'panda.user.jschovan.lsst.%s' % commands.getoutput('uuidgen')
destName    = None

job = JobSpec()
job.jobDefinitionID   = int(time.time()) % 10000
job.jobName           = "%s" % commands.getoutput('uuidgen')
### job.transformation    = 'http://www.usatlas.bnl.gov/~wenaus/lsst-trf/lsst-trf.sh'
job.transformation    = 'http://pandawms.org/pandawms-jobcache/lsst-trf.sh'
job.destinationDBlock = datasetName
#job.destinationSE     = destName
job.destinationSE     = 'local' 
job.currentPriority   = 1000
#job.prodSourceLabel   = 'ptest'
#job.prodSourceLabel = 'panda'
#job.prodSourceLabel = 'ptest'
#job.prodSourceLabel = 'test'
#job.prodSourceLabel = 'ptest'
### 2014-01-27
#job.prodSourceLabel = 'user'
job.prodSourceLabel = 'panda'
job.computingSite     = site
job.jobParameters = ""
job.VO = "lsst"

fileOL = FileSpec()
fileOL.lfn = "%s.job.log.tgz" % job.jobName
fileOL.destinationDBlock = job.destinationDBlock
fileOL.destinationSE     = job.destinationSE
fileOL.dataset           = job.destinationDBlock
fileOL.type = 'log'
job.addFile(fileOL)


s,o = Client.submitJobs([job],srvID=aSrvID)
print s
for x in o:
    print "PandaID=%s" % x[0]

