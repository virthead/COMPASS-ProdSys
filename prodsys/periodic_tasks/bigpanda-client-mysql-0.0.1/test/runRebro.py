import os
import re
import sys
import pytz
import time
import fcntl
import types
import shelve
import random
import datetime
import commands
import threading
import userinterface.Client as Client
from dataservice.DDM import ddm
from dataservice.DDM import dashBorad
from taskbuffer.OraDBProxy import DBProxy
from taskbuffer.TaskBuffer import taskBuffer
from pandalogger.PandaLogger import PandaLogger
from jobdispatcher.Watcher import Watcher
from brokerage.SiteMapper import SiteMapper
from dataservice.Adder import Adder
from dataservice.Finisher import Finisher
from dataservice.MailUtils import MailUtils
from taskbuffer import ProcessGroups
import brokerage.broker_util
import brokerage.broker
import taskbuffer.ErrorCode
import dataservice.DDM

# password
from config import panda_config
passwd = panda_config.dbpasswd

# logger
_logger = PandaLogger().getLogger('runRebro')

_logger.debug("===================== start =====================")

# memory checker
def _memoryCheck(str):
    try:
        proc_status = '/proc/%d/status' % os.getpid()
        procfile = open(proc_status)
        name   = ""
        vmSize = ""
        vmRSS  = ""
        # extract Name,VmSize,VmRSS
        for line in procfile:
            if line.startswith("Name:"):
                name = line.split()[-1]
                continue
            if line.startswith("VmSize:"):
                vmSize = ""
                for item in line.split()[1:]:
                    vmSize += item
                continue
            if line.startswith("VmRSS:"):
                vmRSS = ""
                for item in line.split()[1:]:
                    vmRSS += item
                continue
        procfile.close()
        _logger.debug('MemCheck - %s Name=%s VSZ=%s RSS=%s : %s' % (os.getpid(),name,vmSize,vmRSS,str))
    except:
        type, value, traceBack = sys.exc_info()
        _logger.error("memoryCheck() : %s %s" % (type,value))
        _logger.debug('MemCheck - %s unknown : %s' % (os.getpid(),str))
    return

_memoryCheck("start")

# kill old process
try:
    # time limit
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=7)
    # get process list
    scriptName = sys.argv[0]
    out = commands.getoutput('ps axo user,pid,lstart,args | grep %s' % scriptName)
    for line in out.split('\n'):
        items = line.split()
        # owned process
        if not items[0] in ['sm','atlpan','root']: # ['os.getlogin()']: doesn't work in cron
            continue
        # look for python
        if re.search('python',line) == None:
            continue
        # PID
        pid = items[1]
        # start time
        timeM = re.search('(\S+\s+\d+ \d+:\d+:\d+ \d+)',line)
        startTime = datetime.datetime(*time.strptime(timeM.group(1),'%b %d %H:%M:%S %Y')[:6])
        # kill old process
        if startTime < timeLimit:
            _logger.debug("old process : %s %s" % (pid,startTime))
            _logger.debug(line)            
            commands.getoutput('kill -9 %s' % pid)
except:
    type, value, traceBack = sys.exc_info()
    _logger.error("kill process : %s %s" % (type,value))
    

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

# instantiate sitemapper
siteMapper = SiteMapper(taskBuffer)

_memoryCheck("rebroker")

# rebrokerage
_logger.debug("Rebrokerage start")
try:
    normalTimeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
    sortTimeLimit   = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
    sql  = "SELECT jobDefinitionID,prodUserName,prodUserID,computingSite,MAX(modificationTime) FROM ATLAS_PANDA.jobsActive4 "
    sql += "WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) AND jobStatus=:jobStatus "
    sql += "AND modificationTime<:modificationTime "
    sql += "AND jobsetID IS NOT NULL "    
    sql += "AND processingType IN (:processingType1,:processingType2) "
    sql += "GROUP BY jobDefinitionID,prodUserName,prodUserID,computingSite " 
    varMap = {}
    varMap[':prodSourceLabel1'] = 'user'
    varMap[':prodSourceLabel2'] = 'panda'
    varMap[':modificationTime'] = sortTimeLimit
    varMap[':processingType1']  = 'pathena'
    varMap[':processingType2']  = 'prun'
    varMap[':jobStatus']        = 'activated'
    # get jobs older than threshold
    ret,res = taskBuffer.querySQLS(sql, varMap)
    sql  = "SELECT PandaID,modificationTime FROM %s WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
    sql += "AND modificationTime>:modificationTime AND rownum <= 1"
    if res != None:
        from userinterface.ReBroker import ReBroker
        recentRuntimeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
        # loop over all user/jobID combinations
        iComb = 0
        nComb = len(res)
        _logger.debug("total combinations = %s" % nComb)
        for jobDefinitionID,prodUserName,prodUserID,computingSite,maxModificationTime in res:
            # check time if it is closed to log-rotate
            timeNow  = datetime.datetime.now(pytz.timezone('Europe/Zurich'))
            timeCron = timeNow.replace(hour=4,minute=0,second=0,microsecond=0)
            if (timeNow-timeCron) < datetime.timedelta(seconds=60*10) and \
               (timeCron-timeNow) < datetime.timedelta(seconds=60*30):
                _logger.debug("terminate since close to log-rotate time")
                break
            # check if jobs with the jobID have run recently 
            varMap = {}
            varMap[':prodUserName']     = prodUserName
            varMap[':jobDefinitionID']  = jobDefinitionID
            varMap[':modificationTime'] = recentRuntimeLimit
            _logger.debug(" rebro:%s/%s:ID=%s:%s" % (iComb,nComb,jobDefinitionID,prodUserName))
            iComb += 1
            hasRecentJobs = False
            # check site
            if not siteMapper.checkSite(computingSite):
                _logger.debug("    -> skip unknown site=%s" % computingSite)
                continue
            # check site status            
            tmpSiteStatus = siteMapper.getSite(computingSite).status
            if not tmpSiteStatus in ['offline','test']:
                # use normal time limit for nornal site status
                if maxModificationTime > normalTimeLimit:
                    _logger.debug("    -> skip wait for normal timelimit=%s<maxModTime=%s" % (normalTimeLimit,maxModificationTime))
                    continue
                for tableName in ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsArchived4']: 
                    retU,resU = taskBuffer.querySQLS(sql % tableName, varMap)
                    if resU == None:
                        # database error
                        raise RuntimeError,"failed to check modTime"
                    if resU != []:
                        # found recent jobs
                        hasRecentJobs = True
                        _logger.debug("    -> skip %s ran recently at %s" % (resU[0][0],resU[0][1]))
                        break
            else:
                _logger.debug("    -> immidiate rebro due to site status=%s" % tmpSiteStatus)
            if hasRecentJobs:    
                # skip since some jobs have run recently
                continue
            else:
                reBroker = ReBroker(taskBuffer)
                # try to lock
                rebRet,rebOut = reBroker.lockJob(prodUserID,jobDefinitionID)
                if not rebRet:
                    # failed to lock
                    _logger.debug("    -> failed to lock : %s" % rebOut)
                    continue
                else:
                    # start
                    _logger.debug("    -> start")
                    reBroker.start()
                    reBroker.join()
except:
    errType,errValue = sys.exc_info()[:2]
    _logger.error("rebrokerage failed with %s:%s" % (errType,errValue))

_logger.debug("===================== end =====================")
