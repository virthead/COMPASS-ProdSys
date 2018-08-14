'''
find another candidate site for analysis

'''

import re
import sys
import time
import random
import datetime
import threading

from dataservice.DDM import ddm
from dataservice.DDM import dq2Common
from taskbuffer.JobSpec import JobSpec
from taskbuffer.OraDBProxy import DBProxy
from dataservice.Setupper import Setupper
from brokerage.SiteMapper import SiteMapper
import brokerage.broker

from config import panda_config
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('ReBroker')

def initLogger(pLogger):
    # redirect logging to parent as it doesn't work in nested threads
    global _logger
    _logger = pLogger


class ReBroker (threading.Thread):

    # constructor
    def __init__(self,taskBuffer,cloud=None,excludedSite=None,overrideSite=True,
                 simulation=False,forceOpt=False,userRequest=False,forFailed=False,
                 avoidSameSite=False):
        threading.Thread.__init__(self)
        self.job           = None
        self.jobID         = None
        self.pandaID       = None
        self.cloud         = cloud
        self.pandaJobList  = []
        self.buildStatus   = None
        self.taskBuffer    = taskBuffer
        self.token         = None
        self.newDatasetMap = {}
        self.simulation    = simulation
        self.forceOpt      = forceOpt
        self.excludedSite  = excludedSite
        self.overrideSite = overrideSite
        self.maxPandaIDlibDS = None
        self.userRequest   = userRequest
        self.forFailed     = forFailed
        self.revNum        = 0
        self.avoidSameSite = avoidSameSite
        self.brokerageInfo = []


    # main
    def run(self):
        try:
            # get job
            tmpJobs = self.taskBuffer.getFullJobStatus([self.rPandaID])
            if tmpJobs == [] or tmpJobs[0] == None:
                _logger.debug("cannot find job for PandaID=%s" % self.rPandaID)
                return
            self.job = tmpJobs[0]
            _logger.debug("%s start %s:%s:%s" % (self.token,self.job.jobDefinitionID,self.job.prodUserName,self.job.computingSite))
            # using output container
            if not self.job.destinationDBlock.endswith('/'):
                _logger.debug("%s ouput dataset container is required" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # FIXEME : dont' touch group jobs for now
            if self.job.destinationDBlock.startswith('group') and (not self.userRequest):
                _logger.debug("%s skip group jobs" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # check processingType
            typesForRebro = ['pathena','prun','ganga','ganga-rbtest']
            if not self.job.processingType in typesForRebro:
                _logger.debug("%s skip processingType=%s not in %s" % \
                              (self.token,self.job.processingType,str(typesForRebro)))
                _logger.debug("%s end" % self.token)
                return
            # check jobsetID
            if self.job.jobsetID in [0,'NULL',None]:
                _logger.debug("%s jobsetID is undefined" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # check metadata 
            if self.job.metadata in [None,'NULL']:
                _logger.debug("%s metadata is unavailable" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # check --disableRebrokerage
            match = re.search("--disableRebrokerage",self.job.metadata)
            if match != None and (not self.simulation) and (not self.forceOpt) \
                   and (not self.userRequest):
                _logger.debug("%s diabled rebrokerage" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # check --site
            match = re.search("--site",self.job.metadata)
            if match != None and (not self.simulation) and (not self.forceOpt) \
                   and (not self.userRequest):
                _logger.debug("%s --site is used" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # check --libDS
            match = re.search("--libDS",self.job.metadata)
            if match != None:
                _logger.debug("%s --libDS is used" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # check --workingGroup since it is site-specific 
            match = re.search("--workingGroup",self.job.metadata)
            if match != None:
                _logger.debug("%s workingGroup is specified" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # avoid too many rebrokerage
            if not self.checkRev():
                _logger.debug("%s avoid too many rebrokerage" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # check if multiple JobIDs use the same libDS
            if self.bPandaID != None and self.buildStatus not in ['finished','failed']:
                if self.minPandaIDlibDS == None or self.maxPandaIDlibDS == None:
                    _logger.debug("%s max/min PandaIDs are unavailable for the libDS" % self.token)
                    _logger.debug("%s end" % self.token)
                    return
                tmpPandaIDsForLibDS = self.taskBuffer.getFullJobStatus([self.minPandaIDlibDS,self.maxPandaIDlibDS])
                if len(tmpPandaIDsForLibDS) != 2 or tmpPandaIDsForLibDS[0] == None or tmpPandaIDsForLibDS[1] == None:
                    _logger.debug("%s failed to get max/min PandaIDs for the libDS" % self.token)
                    _logger.debug("%s end" % self.token)
                    return
                # check
                if tmpPandaIDsForLibDS[0].jobDefinitionID != tmpPandaIDsForLibDS[1].jobDefinitionID:
                    _logger.debug("%s multiple JobIDs use the libDS %s:%s %s:%s" % (self.token,tmpPandaIDsForLibDS[0].jobDefinitionID,
                                                                                    self.minPandaIDlibDS,tmpPandaIDsForLibDS[1].jobDefinitionID,
                                                                                    self.maxPandaIDlibDS))
                    _logger.debug("%s end" % self.token)
                    return
            # check excludedSite
            if self.excludedSite == None:
                self.excludedSite = []
                match = re.search("--excludedSite( +|=)\s*(\'|\")*([^ \"\';$]+)",self.job.metadata)
                if match != None:
                    self.excludedSite = match.group(3).split(',')
            # remove empty
            try:
                self.excludedSite.remove('')
            except:
                pass
            _logger.debug("%s excludedSite=%s" % (self.token,str(self.excludedSite)))
            # check cloud
            if self.cloud == None:
                match = re.search("--cloud( +|=)\s*(\'|\")*([^ \"\';$]+)",self.job.metadata)
                if match != None:
                    self.cloud = match.group(3)
            _logger.debug("%s cloud=%s" % (self.token,self.cloud))
            # get inDS/LFNs
            status,tmpMapInDS,maxFileSize = self.taskBuffer.getInDatasetsForReBrokerage(self.jobID,self.userName)
            if not status:
                # failed
                _logger.error("%s failed to get inDS/LFN from DB" % self.token)
                return
            status,inputDS = self.getListDatasetsUsedByJob(tmpMapInDS)
            if not status:
                # failed
                _logger.error("%s failed" % self.token)
                return 
            # get relicas
            replicaMap = {}
            unknownSites = {} 
            for tmpDS in inputDS:
                if tmpDS.endswith('/'):
                    # container
                    status,tmpRepMaps = self.getListDatasetReplicasInContainer(tmpDS)
                else:
                    # normal dataset
                    status,tmpRepMap = self.getListDatasetReplicas(tmpDS)
                    tmpRepMaps = {tmpDS:tmpRepMap}
                if not status:
                    # failed
                    _logger.debug("%s failed" % self.token)
                    return 
                # make map per site
                for tmpDS,tmpRepMap in tmpRepMaps.iteritems():
                    for tmpSite,tmpStat in tmpRepMap.iteritems():
                        # ignore special sites
                        if tmpSite in ['CERN-PROD_TZERO','CERN-PROD_DAQ','CERN-PROD_TMPDISK']:
                            continue
                        # ignore tape sites
                        if tmpSite.endswith('TAPE'):
                            continue
                        # keep sites with unknown replica info 
                        if tmpStat[-1]['found'] == None:
                            if not unknownSites.has_key(tmpDS):
                                unknownSites[tmpDS] = []
                            unknownSites[tmpDS].append(tmpSite)
                        # ignore ToBeDeleted
                        if tmpStat[-1]['archived'] in ['ToBeDeleted',]:
                            continue
                        # change EOS
                        if tmpSite.startswith('CERN-PROD_EOS'):
                            tmpSite = 'CERN-PROD_EOS'
                        # change EOS TMP
                        if tmpSite.startswith('CERN-PROD_TMP'):
                            tmpSite = 'CERN-PROD_TMP'
                        # change DISK to SCRATCHDISK
                        tmpSite = re.sub('_[^_-]+DISK$','',tmpSite)
                        # change PERF-XYZ to SCRATCHDISK
                        tmpSite = re.sub('_PERF-[^_-]+$','',tmpSite)
                        # change PHYS-XYZ to SCRATCHDISK
                        tmpSite = re.sub('_PHYS-[^_-]+$','',tmpSite)
                        # patch for BNLPANDA
                        if tmpSite in ['BNLPANDA']:
                            tmpSite = 'BNL-OSG2'
                        # add to map    
                        if not replicaMap.has_key(tmpSite):
                            replicaMap[tmpSite] = {}
                        replicaMap[tmpSite][tmpDS] = tmpStat[-1]
            _logger.debug("%s replica map -> %s" % (self.token,str(replicaMap)))
            # refresh replica info in needed
            self.refreshReplicaInfo(unknownSites)
            # instantiate SiteMapper
            siteMapper = SiteMapper(self.taskBuffer)
            # get original DDM
            origSiteDDM = self.getAggName(siteMapper.getSite(self.job.computingSite).ddm)
            # check all datasets
            maxDQ2Sites = []
            if inputDS != []:
                # loop over all sites
                for tmpSite,tmpDsVal in replicaMap.iteritems():
                    # loop over all datasets
                    appendFlag = True
                    for tmpOrigDS in inputDS:
                        # check completeness
                        if tmpDsVal.has_key(tmpOrigDS) and tmpDsVal[tmpOrigDS]['found'] != None and \
                               tmpDsVal[tmpOrigDS]['total'] == tmpDsVal[tmpOrigDS]['found']:
                            pass
                        else:
                            appendFlag = False
                    # append
                    if appendFlag:
                        if not tmpSite in maxDQ2Sites:
                            maxDQ2Sites.append(tmpSite)
            _logger.debug("%s candidate DQ2s -> %s" % (self.token,str(maxDQ2Sites)))
            if inputDS != [] and maxDQ2Sites == []:
                _logger.debug("%s no DQ2 candidate" % self.token)
            else:
                maxPandaSites = []
                # original maxinputsize
                origMaxInputSize = siteMapper.getSite(self.job.computingSite).maxinputsize
                # look for Panda siteIDs
                for tmpSiteID,tmpSiteSpec in siteMapper.siteSpecList.iteritems():
                    # use ANALY_ only
                    if not tmpSiteID.startswith('ANALY_'):
                        continue
                    # remove test and local
                    if re.search('_test',tmpSiteID,re.I) != None:
                        continue
                    if re.search('_local',tmpSiteID,re.I) != None:
                        continue
                    # avoid same site
                    if self.avoidSameSite and self.getAggName(tmpSiteSpec.ddm) == origSiteDDM:
                        continue
                    # check DQ2 ID
                    if self.cloud in [None,tmpSiteSpec.cloud] \
                           and (self.getAggName(tmpSiteSpec.ddm) in maxDQ2Sites or inputDS == []):
                        # excluded sites
                        excludedFlag = False
                        for tmpExcSite in self.excludedSite:
                            if re.search(tmpExcSite,tmpSiteID) != None:
                                excludedFlag = True
                                break
                        if excludedFlag:
                            _logger.debug("%s skip %s since excluded" % (self.token,tmpSiteID))
                            continue
                        # use online only
                        if tmpSiteSpec.status != 'online':
                            _logger.debug("%s skip %s status=%s" % (self.token,tmpSiteID,tmpSiteSpec.status))
                            continue
                        # check maxinputsize
                        if (maxFileSize == None and origMaxInputSize > siteMapper.getSite(tmpSiteID).maxinputsize) or \
                               maxFileSize > siteMapper.getSite(tmpSiteID).maxinputsize:
                            _logger.debug("%s skip %s due to maxinputsize" % (self.token,tmpSiteID))
                            continue
                        # append
                        if not tmpSiteID in maxPandaSites:
                            maxPandaSites.append(tmpSiteID)
                # choose at most 20 sites randomly to avoid too many lookup            
                random.shuffle(maxPandaSites)
                maxPandaSites = maxPandaSites[:20]
                _logger.debug("%s candidate PandaSites -> %s" % (self.token,str(maxPandaSites)))
                # no Panda siteIDs            
                if maxPandaSites == []:            
                    _logger.debug("%s no Panda site candidate" % self.token)
                else:
                    # set AtlasRelease and cmtConfig to dummy job
                    tmpJobForBrokerage = JobSpec()
                    if self.job.AtlasRelease in ['NULL',None]:
                        tmpJobForBrokerage.AtlasRelease = ''
                    else:
                        tmpJobForBrokerage.AtlasRelease = self.job.AtlasRelease
                    # use nightlies
                    matchNight = re.search('^AnalysisTransforms-.*_(rel_\d+)$',self.job.homepackage)
                    if matchNight != None:
                        tmpJobForBrokerage.AtlasRelease += ':%s' % matchNight.group(1)
                    # use cache
                    else:
                        matchCache = re.search('^AnalysisTransforms-([^/]+)',self.job.homepackage)
                        if matchCache != None:
                            tmpJobForBrokerage.AtlasRelease = matchCache.group(1).replace('_','-')
                    if not self.job.cmtConfig in ['NULL',None]:    
                        tmpJobForBrokerage.cmtConfig = self.job.cmtConfig
                    # memory size
                    if not self.job.minRamCount in ['NULL',None,0]:
                        tmpJobForBrokerage.minRamCount = self.job.minRamCount
                    # CPU count
                    if not self.job.maxCpuCount in ['NULL',None,0]:
                        tmpJobForBrokerage.maxCpuCount = self.job.maxCpuCount
                    # run brokerage
                    brokerage.broker.schedule([tmpJobForBrokerage],self.taskBuffer,siteMapper,forAnalysis=True,
                                              setScanSiteList=maxPandaSites,trustIS=True,reportLog=True)
                    newSiteID = tmpJobForBrokerage.computingSite
                    self.brokerageInfo += tmpJobForBrokerage.brokerageErrorDiag
                    _logger.debug("%s runBrokerage - > %s" % (self.token,newSiteID))
                    # unknown site
                    if not siteMapper.checkSite(newSiteID):
                        _logger.error("%s unknown site" % self.token)
                        _logger.debug("%s failed" % self.token)
                        return 
                    # get new site spec
                    newSiteSpec = siteMapper.getSite(newSiteID)
                    # avoid repetition
                    if self.getAggName(newSiteSpec.ddm) == origSiteDDM:
                        _logger.debug("%s assigned to the same site %s " % (self.token,newSiteID))
                        _logger.debug("%s end" % self.token)                        
                        return
                    # simulation mode
                    if self.simulation:
                        _logger.debug("%s end simulation" % self.token)                        
                        return
                    # prepare jobs
                    status = self.prepareJob(newSiteID,newSiteSpec.cloud)
                    if status:
                        # run SetUpper
                        statusSetUp = self.runSetUpper()
                        if not statusSetUp:
                            _logger.debug("%s runSetUpper failed" % self.token)
                        else:
                            _logger.debug("%s successfully assigned to %s" % (self.token,newSiteID))
            _logger.debug("%s end" % self.token)
        except:
            errType,errValue,errTraceBack = sys.exc_info()
            _logger.error("%s run() : %s %s" % (self.token,errType,errValue))


    # get aggregated DQ2 ID
    def getAggName(self,origName):
        if origName.startswith('CERN-PROD_EOS'):
            return 'CERN-PROD_EOS'
        if origName.startswith('CERN-PROD_TMP'):
            return 'CERN-PROD_TMP'
        return re.sub('_[^_-]+DISK$','',origName)
    
        
    # lock job to disable multiple broker running in parallel
    def lockJob(self,dn,jobID):
        # make token
        tmpProxy = DBProxy()
        self.token = "%s:%s:" % (tmpProxy.cleanUserID(dn),jobID)
        _logger.debug("%s lockJob" % self.token)        
        # lock
        resST,resVal = self.taskBuffer.lockJobForReBrokerage(dn,jobID,self.simulation,self.forceOpt,
                                                             forFailed=self.forFailed)
        # failed
        if not resST:
            _logger.debug("%s lockJob failed since %s" % (self.token,resVal['err']))
            return False,resVal['err']
        # keep jobID
        self.jobID = jobID
        # set PandaID,buildStatus,userName
        self.rPandaID    = resVal['rPandaID']        
        self.bPandaID    = resVal['bPandaID']
        self.userName    = resVal['userName']
        self.buildStatus = resVal['bStatus']
        self.buildJobID  = resVal['bJobID']
        self.minPandaIDlibDS = resVal['minPandaIDlibDS']
        self.maxPandaIDlibDS = resVal['maxPandaIDlibDS']
        # use JobID as rev num
        self.revNum = self.taskBuffer.getJobIdUser(dn)
        _logger.debug("%s run PandaID=%s / build PandaID=%s Status=%s JobID=%s rev=%s" % \
                      (self.token,self.rPandaID,self.bPandaID,self.buildStatus,
                       self.buildJobID,self.revNum))
        # return
        return True,''


    # move build job to jobsDefined4
    def prepareJob(self,site,cloud):
        _logger.debug("%s prepareJob" % self.token)
        # reuse buildJob + all runJobs
        if self.jobID == self.buildJobID and self.buildStatus in ['defined','activated']:
            if self.buildStatus == 'activated':
                # move build job to jobsDefined4                
                ret = self.taskBuffer.resetBuildJobForReBrokerage(self.bPandaID)
                if not ret:
                    _logger.error("%s failed to move build job %s to jobsDefined" % (self.token,self.bPandaID))
                    return False
            # get PandaIDs from jobsDefined4    
            tmpPandaIDs = self.taskBuffer.getPandaIDsForReBrokerage(self.userName,self.jobID,False,
                                                                    forFailed=self.forFailed)
            if tmpPandaIDs == []:
                _logger.error("%s cannot find PandaDSs" % self.token)
                return False
            # get jobSpecs
            iBunchJobs = 0
            nBunchJobs = 500
            tmpJobsMap = {}
            while iBunchJobs < len(tmpPandaIDs):
                # get IDs
                tmpJobs = self.taskBuffer.peekJobs(tmpPandaIDs[iBunchJobs:iBunchJobs+nBunchJobs],True,False,False,False)
                for tmpJob in tmpJobs:
                    if tmpJob != None and tmpJob.jobStatus in ['defined','assigned']:
                        # remove _sub suffix
                        for tmpFile in tmpJob.Files:
                            if tmpFile.type != 'input':
                                tmpFile.destinationDBlock = re.sub('_sub\d+$','',tmpFile.destinationDBlock)
                        self.pandaJobList.append(tmpJob)
                # increment index
                iBunchJobs += nBunchJobs
        # make new bunch
        else:
            # make new buildJob
            if self.bPandaID != None:            
                tmpJobs = self.taskBuffer.getFullJobStatus([self.bPandaID])
                if tmpJobs == [] or tmpJobs[0] == None:
                    _logger.debug("cannot find build job for PandaID=%s" % self.bPandaID)
                    return False
                # make
                tmpBuildJob,oldLibDS,newLibDS = self.makeNewBuildJobForRebrokerage(tmpJobs[0])
                # set parameters
                tmpBuildJob.jobExecutionID = self.jobID
                tmpBuildJob.jobsetID       = -1
                tmpBuildJob.sourceSite     = self.job.jobsetID
                # register
                status = self.registerNewDataset(newLibDS)
                if not status:
                    _logger.debug("%s failed to register new libDS" % self.token)
                    return False
                # append
                self.pandaJobList = [tmpBuildJob]
            # prepare outputDS
            status = self.prepareDS()
            if not status:
                _logger.error("%s failed to prepare outputDS" % self.token)
                return False
            # get PandaIDs
            if self.buildStatus in ['finished',None]:
                # from jobsActivated when buildJob already finished or noBuild
                tmpPandaIDs = self.taskBuffer.getPandaIDsForReBrokerage(self.userName,self.jobID,True,
                                                                        forFailed=self.forFailed)                
            else:
                # from jobsDefined
                tmpPandaIDs = self.taskBuffer.getPandaIDsForReBrokerage(self.userName,self.jobID,False,
                                                                        forFailed=self.forFailed)
            if tmpPandaIDs == []:
                _logger.error("%s cannot find PandaDSs" % self.token)
                return False
            # get jobSpecs
            iBunchJobs = 0
            nBunchJobs = 500
            tmpJobsMap = {}
            while iBunchJobs < len(tmpPandaIDs):
                # get jobs
                tmpJobs = self.taskBuffer.peekJobs(tmpPandaIDs[iBunchJobs:iBunchJobs+nBunchJobs],True,True,False,False,True)
                for tmpJob in tmpJobs:
                    # reset parameters for retry
                    if self.forFailed and tmpJob != None:
                        self.taskBuffer.retryJob(tmpJob.PandaID,{},failedInActive=True,
                                                 changeJobInMem=True,inMemJob=tmpJob)
                        # set holding to be compatible with rebro jobs
                        tmpJob.jobStatus = 'holding'
                    # check job status. activated jobs were changed to holding by getPandaIDsForReBrokerage
                    if tmpJob != None and tmpJob.jobStatus in ['defined','assigned','holding']:
                        # reset parameter
                        tmpJob.parentID = tmpJob.PandaID
                        tmpJob.PandaID = None
                        tmpJob.jobExecutionID = tmpJob.jobDefinitionID
                        tmpJob.jobsetID       = -1
                        tmpJob.sourceSite     = self.job.jobsetID
                        if self.bPandaID != None:
                            tmpJob.jobParameters = re.sub(oldLibDS,newLibDS,tmpJob.jobParameters)
                        for tmpFile in tmpJob.Files:
                            tmpFile.row_ID = None
                            tmpFile.PandaID = None
                            if tmpFile.type == 'input':
                                if self.bPandaID != None and tmpFile.dataset == oldLibDS:
                                    tmpFile.status  = 'unknown'
                                    tmpFile.GUID    = None
                                    tmpFile.dataset = newLibDS
                                    tmpFile.dispatchDBlock = newLibDS
                                    tmpFile.lfn = re.sub(oldLibDS,newLibDS,tmpFile.lfn)
                            else:
                                # use new dataset
                                tmpFile.destinationDBlock = re.sub('_sub\d+$','',tmpFile.destinationDBlock)
                                if not self.newDatasetMap.has_key(tmpFile.destinationDBlock):
                                    _logger.error("%s cannot find new dataset for %s:%s" % (self.token,tmpFile.PandaID,tmpFile.destinationDBlock))
                                    return False
                                tmpFile.destinationDBlock = self.newDatasetMap[tmpFile.destinationDBlock] 
                        # append
                        self.pandaJobList.append(tmpJob)
                # increment index
                iBunchJobs += nBunchJobs
        # no jobs
        if self.pandaJobList == []:
            _logger.error("%s no jobs" % self.token)
            return False
        # set cloud, site, and specialHandling 
        for tmpJob in self.pandaJobList:
            # set specialHandling
            if tmpJob.specialHandling in [None,'NULL','']:
                if not self.forFailed:
                    tmpJob.specialHandling = 'rebro'
                else:
                    tmpJob.specialHandling = 'sretry'                    
            else:
                if not self.forFailed:
                    tmpJob.specialHandling += ',rebro'
                else:    
                    tmpJob.specialHandling += ',sretry'                    
            # check if --destSE is used
            oldComputingSite = tmpJob.computingSite
            if tmpJob.destinationSE == oldComputingSite:
                tmpJob.destinationSE = site
            # set site and cloud    
            tmpJob.computingSite = site
            tmpJob.cloud = cloud
            # reset destinationDBlock
            for tmpFile in tmpJob.Files:
                if tmpFile.type in ['output','log']:
                    # set destSE
                    if tmpFile.destinationSE == oldComputingSite:
                        tmpFile.destinationSE = site
        # set the same specialHandling since new build may have different specialHandling
        self.pandaJobList[0].specialHandling = self.pandaJobList[-1].specialHandling
        # return
        return True

        
    # prepare libDS 
    def prepareDS(self):
        _logger.debug("%s prepareDS" % self.token)        
        # get all outDSs
        shadowDsName = None
        for tmpFile in self.job.Files:
            if tmpFile.type in ['output','log']:
                tmpDS = re.sub('_sub\d+$','',tmpFile.destinationDBlock)
                # append new rev number
                match = re.search('_rev(\d+)$',tmpDS)
                if match == None:
                    newDS = tmpDS + '_rev%s' % self.revNum
                else:
                    newDS = re.sub('_rev(\d+)$','_rev%s' % self.revNum,tmpDS)
                # add shadow
                """
                if shadowDsName == None and tmpFile.type == 'log':
                    shadowDsName = "%s_shadow" % newDS
                    status = self.registerNewDataset(shadowDsName)
                    if not status:
                        _logger.debug("%s prepareDS failed for shadow" % self.token)
                        return False
                """        
                # add datasets                                                    
                if not tmpDS in self.newDatasetMap:
                    # register
                    status = self.registerNewDataset(newDS,tmpFile.dataset)
                    if not status:
                        _logger.debug("%s prepareDS failed" % self.token)                                
                        return False
                    # append
                    self.newDatasetMap[tmpDS] = newDS
        return True            


    # run SetUpper
    def runSetUpper(self):
        # reuse buildJob + all runJobs
        reuseFlag = False
        if self.jobID == self.buildJobID and self.buildStatus in ['defined','activated']:
            reuseFlag = True
            _logger.debug("%s start Setupper for JobID=%s" % (self.token,self.jobID))
            thr = Setupper(self.taskBuffer,self.pandaJobList,resetLocation=True)
            thr.start()
            thr.join()
        # new bunch    
        else:
            # fake FQANs
            fqans = []
            if not self.job.countryGroup in ['','NULL',None]:
                fqans.append('/atlas/%s/Role=NULL' % self.job.countryGroup)
            if self.job.destinationDBlock.startswith('group') and not self.job.workingGroup in ['','NULL',None]:
                fqans.append('/atlas/%s/Role=production' % self.job.workingGroup)
            # insert jobs
            _logger.debug("%s start storeJobs for JobID=%s" % (self.token,self.jobID))            
            ret = self.taskBuffer.storeJobs(self.pandaJobList,self.job.prodUserID,True,False,fqans,
                                            self.job.creationHost,True,checkSpecialHandling=False)
            if ret == []:
                _logger.error("%s storeJobs failed with [] for JobID=%s" % (self.token,self.jobID))
                return False
            # get PandaIDs to be killed
            pandaIDsTobeKilled = []
            newJobDefinitionID = None
            newJobsetID = None
            strNewIDsList = []
            for tmpIndex,tmpItem in enumerate(ret):
                if not tmpItem[0] in ['NULL',None]:
                    tmpJob = self.pandaJobList[tmpIndex]
                    if not tmpJob.parentID in [0,None,'NULL']:                    
                        pandaIDsTobeKilled.append(tmpJob.parentID)
                        if newJobDefinitionID == None:
                            newJobDefinitionID = tmpItem[1]
                        if newJobsetID == None:
                            newJobsetID = tmpItem[2]['jobsetID']
                        strNewIDs = 'PandaID=%s JobsetID=%s JobID=%s' % (tmpItem[0],newJobsetID,newJobDefinitionID)
                        strNewIDsList.append(strNewIDs)
            if pandaIDsTobeKilled != []:
                strNewJobIDs = "JobsetID=%s JobID=%s" % (newJobsetID,newJobDefinitionID)
                _logger.debug("%s kill jobs for JobID=%s -> new %s : %s" % \
                              (self.token,self.jobID,strNewJobIDs,str(pandaIDsTobeKilled)))
                for tmpIdx,tmpPandaID in enumerate(pandaIDsTobeKilled):
                    if not self.forFailed:
                        self.taskBuffer.killJobs([tmpPandaID],strNewIDsList[tmpIdx],'8',True)
                    else:
                        self.taskBuffer.killJobs([tmpPandaID],strNewIDsList[tmpIdx],'7',True)
        # send brokerage info
        if not self.forFailed: 
            tmpMsg = 'action=rebrokerage ntry=%s ' % self.pandaJobList[0].specialHandling.split(',').count('rebro')
        else:
            tmpMsg = 'action=serverretry ntry=%s ' % self.pandaJobList[0].specialHandling.split(',').count('sretry')
        tmpMsg += 'old_jobset=%s old_jobdef=%s old_site=%s' % (self.job.jobsetID,self.jobID,self.job.computingSite)    
        self.brokerageInfo.append(tmpMsg)
        brokerage.broker.sendMsgToLoggerHTTP(self.brokerageInfo,self.pandaJobList[0])
        # succeeded
        _logger.debug("%s completed for JobID=%s" % (self.token,self.jobID))
        return True

    
    # check DDM response
    def isDQ2ok(self,out):
        if out.find("DQ2 internal server exception") != -1 \
               or out.find("An error occurred on the central catalogs") != -1 \
               or out.find("MySQL server has gone away") != -1 \
               or out == '()':
            return False
        return True
    

    # get list of datasets
    def getListDatasets(self,dataset):
        nTry = 3
        for iDDMTry in range(nTry):
            _logger.debug("%s %s/%s listDatasets %s" % (self.token,iDDMTry,nTry,dataset))
            status,out =  ddm.DQ2.main('listDatasets',dataset,0,True)            
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        # result    
        if status != 0 or out.startswith('Error'):
            _logger.error(self.token+' '+out)
            _logger.error('%s bad DQ2 response for %s' % (self.token,dataset))            
            return False,{}
        try:
            # convert res to map
            exec "tmpDatasets = %s" % out
            # remove _sub/_dis
            resList = []
            for tmpDS in tmpDatasets.keys():
                if re.search('(_sub|_dis)\d+$',tmpDS) == None and re.search('(_shadow$',tmpDS) == None:
                    resList.append(tmpDS)
            _logger.debug('%s getListDatasets->%s' % (self.token,str(resList)))
            return True,resList
        except:
            _logger.error(self.token+' '+out)            
            _logger.error('%s could not convert HTTP-res to datasets for %s' % (self.token,dataset))
            return False,{}

            
    # get list of replicas for a dataset
    def getListDatasetReplicas(self,dataset):
        nTry = 3
        for iDDMTry in range(nTry):
            _logger.debug("%s %s/%s listDatasetReplicas %s" % (self.token,iDDMTry,nTry,dataset))
            status,out = ddm.DQ2.main('listDatasetReplicas',dataset,0,None,False)
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        # result    
        if status != 0 or out.startswith('Error'):
            _logger.error(self.token+' '+out)
            _logger.error('%s bad DQ2 response for %s' % (self.token,dataset))            
            return False,{}
        try:
            # convert res to map
            exec "tmpRepSites = %s" % out
            _logger.debug('%s getListDatasetReplicas->%s' % (self.token,str(tmpRepSites)))
            return True,tmpRepSites
        except:
            _logger.error(self.token+' '+out)            
            _logger.error('%s could not convert HTTP-res to replica map for %s' % (self.token,dataset))
            return False,{}
        
    
    # get replicas for a container 
    def getListDatasetReplicasInContainer(self,container):
        # response for failure
        resForFailure = False,{}
        # get datasets in container
        nTry = 3
        for iDDMTry in range(nTry):
            _logger.debug('%s %s/%s listDatasetsInContainer %s' % (self.token,iDDMTry,nTry,container))
            status,out = ddm.DQ2.main('listDatasetsInContainer',container)
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            _logger.error(self.token+' '+out)
            _logger.error('%s bad DQ2 response for %s' % (self.token,container))
            return resForFailure
        datasets = []
        try:
            # convert to list
            exec "datasets = %s" % out
        except:
            _logger.error('%s could not convert HTTP-res to dataset list for %s' % (self.token,container))
            return resForFailure
        # loop over all datasets
        allRepMap = {}
        for dataset in datasets:
            # get replicas
            status,tmpRepSites = self.getListDatasetReplicas(dataset)
            if not status:
                return resForFailure
            # append
            allRepMap[dataset] = tmpRepSites
        # return
        _logger.debug('%s getListDatasetReplicasInContainer done')
        return True,allRepMap            


    # delete original locations
    def deleteDatasetReplicas(self,datasets):
        # loop over all datasets
        for dataset in datasets:
            # get locations
            status,tmpRepSites = self.getListDatasetReplicas(dataset)
            if not status:
                return False
            # no replicas
            if len(tmpRepSites.keys()) == 0:
                continue
            # delete
            nTry = 3
            for iDDMTry in range(nTry):
                _logger.debug("%s %s/%s deleteDatasetReplicas %s" % (self.token,iDDMTry,nTry,dataset))
                status,out = ddm.DQ2.main('deleteDatasetReplicas',dataset,tmpRepSites.keys())
                if status != 0 or (not self.isDQ2ok(out)):
                    time.sleep(60)
                else:
                    break
            # result
            if status != 0 or out.startswith('Error'):
                _logger.error(self.token+' '+out)
                _logger.error('%s bad DQ2 response for %s' % (self.token,dataset))            
                return False
            _logger.debug(self.token+' '+out)
        # return
        _logger.debug('%s deleted replicas for %s' % (self.token,str(datasets)))
        return True


    # check if datasets are empty
    def checkDatasetContents(self,datasets):
        # loop over all datasets
        for dataset in datasets:
            # check
            nTry = 3
            for iDDMTry in range(nTry):
                _logger.debug("%s %s/%s getNumberOfFiles %s" % (self.token,iDDMTry,nTry,dataset))
                status,out = ddm.DQ2.main('getNumberOfFiles',dataset)
                if status != 0 or (not self.isDQ2ok(out)):
                    time.sleep(60)
                else:
                    break
            # result
            if status != 0 or out.startswith('Error'):
                _logger.error(self.token+' '+out)
                _logger.error('%s bad DQ2 response for %s' % (self.token,dataset))            
                return False
            # convert to int
            _logger.debug(self.token+' '+out)            
            try:
                nFile = int(out)
                # not empty
                if nFile != 0:
                    _logger.error('%s %s is not empty' % (self.token,dataset))            
                    return False
            except:
                _logger.error("%s could not convert HTTP-res to nFiles" % (self.token,dataset))
                return False
        # all OK
        return True
                

    # register dataset
    def registerNewDataset(self,dataset,container=''):
        nTry = 3
        for iDDMTry in range(nTry):
            _logger.debug("%s %s/%s registerNewDataset %s" % (self.token,iDDMTry,nTry,dataset))
            status,out = ddm.DQ2.main('registerNewDataset',dataset)
            if out.find('DQDatasetExistsException') != -1:
                break
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        # result
        if out.find('DQDatasetExistsException') != -1:
            # ignore DQDatasetExistsException
            pass
        elif status != 0 or out.startswith('Error'):
            _logger.error(self.token+' '+out)
            _logger.error('%s failed to register new dataset %s' % (self.token,dataset))            
            return False
        # remove /CN=proxy and /CN=limited from DN
        tmpRealDN = self.job.prodUserID
        tmpRealDN = re.sub('/CN=limited proxy','',tmpRealDN)
        tmpRealDN = re.sub('/CN=proxy','',tmpRealDN)
        status,out = dq2Common.parse_dn(tmpRealDN)
        if status != 0:
            _logger.error(self.token+' '+out)
            _logger.error('%s failed to truncate DN:%s' % (self.token,self.job.prodUserID))
            return False
        tmpRealDN = out
        # set owner
        for iDDMTry in range(nTry):
            _logger.debug("%s %s/%s setMetaDataAttribute %s %s" % (self.token,iDDMTry,nTry,dataset,tmpRealDN))
            status,out = ddm.DQ2.main('setMetaDataAttribute',dataset,'owner',tmpRealDN)            
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            _logger.error(self.token+' '+out)
            _logger.error('%s failed to set owner to dataset %s' % (self.token,dataset))            
            return False
        # add to contaner
        if container != '' and container.endswith('/'):
            for iDDMTry in range(nTry):
                _logger.debug("%s %s/%s registerDatasetsInContainer %s to %s" % (self.token,iDDMTry,nTry,dataset,container))
                status,out = ddm.DQ2.main('registerDatasetsInContainer',container,[dataset])
                if out.find('DQContainerAlreadyHasDataset') != -1:
                    break
                if status != 0 or (not self.isDQ2ok(out)):
                    time.sleep(60)
                else:
                    break
            if out.find('DQContainerAlreadyHasDataset') != -1:
                # ignore DQContainerAlreadyHasDataset
                pass
            elif status != 0 or out.startswith('Error'):
                _logger.error(self.token+' '+out)
                _logger.error('%s add %s to container:%s' % (self.token,dataset,container))            
                return False
        # return
        return True
                    

    # get list of dataset used by the job             
    def getListDatasetsUsedByJob(self,mapDsLFN):
        # response for failure
        resForFailure = False,[]
        # loop over all datasets
        retList = []
        for tmpDsContainer,tmpLFNs in mapDsLFN.iteritems():
            # not a container
            if not tmpDsContainer.endswith('/'):
                if not tmpDsContainer in retList:
                    retList.append(tmpDsContainer)
                continue
            # get datasets in container
            nTry = 3
            for iDDMTry in range(nTry):
                _logger.debug('%s %s/%s listDatasetsInContainer %s' % (self.token,iDDMTry,nTry,tmpDsContainer))
                status,out = ddm.DQ2.main('listDatasetsInContainer',tmpDsContainer)
                if status != 0 or (not self.isDQ2ok(out)):
                    time.sleep(60)
                else:
                    break
            if status != 0 or out.startswith('Error'):
                _logger.error(self.token+' '+out)
                _logger.error('%s bad DQ2 response for %s' % (self.token,tmpDsContainer))
                return resForFailure
            tmpDatasets = []
            try:
                # convert to list
                exec "tmpDatasets = %s" % out
            except:
                _logger.error('%s could not convert HTTP-res to dataset list for %s' % (self.token,tmpDsContainer))
                return resForFailure
            # get files in dataset
            for tmpDS in tmpDatasets:
                if tmpDS in retList:
                    continue
                nTry = 3
                for iDDMTry in range(nTry):
                    _logger.debug('%s %s/%s listFilesInDataset %s' % (self.token,iDDMTry,nTry,tmpDS))
                    status,out = ddm.DQ2.main('listFilesInDataset',tmpDS)
                    if status != 0 or (not self.isDQ2ok(out)):
                        time.sleep(60)
                    else:
                        break
                if status != 0 or out.startswith('Error'):
                    _logger.error(self.token+' '+out)
                    _logger.error('%s bad DQ2 response for %s' % (self.token,tmpDS))
                    return resForFailure
                # get LFN map
                tmpMapDQ2 = {}
                try:
                    # convert to list
                    exec "tmpMapDQ2 = %s[0]" % out
                    for tmpGUID,tmpVal in tmpMapDQ2.iteritems():
                        # check if a file in DS is used by the job
                        if tmpVal['lfn'] in tmpLFNs:
                            # append
                            if not tmpDS in retList:
                                retList.append(tmpDS)
                            break
                except:
                    _logger.error('%s could not convert HTTP-res to LFN map for %s' % (self.token,tmpDS))
                    return resForFailure
        # return
        _logger.debug('%s getListDatasetsUsedByJob done %s' % (self.token,str(retList)))
        return True,retList
            

    # refresh replica info in needed
    def refreshReplicaInfo(self,unknownSites):
        for tmpDS,sites in unknownSites.iteritems():
            nTry = 3
            for iDDMTry in range(nTry):
                _logger.debug("%s %s/%s listFileReplicasBySites %s %s" % (self.token,iDDMTry,nTry,tmpDS,str(sites)))
                status,out =  ddm.DQ2_iter.listFileReplicasBySites(tmpDS,0,sites,0,300)
                if status != 0 or (not self.isDQ2ok(out)):
                    time.sleep(60)
                else:
                    break
            # result    
            if status != 0 or out.startswith('Error'):
                _logger.error(self.token+' '+out)
                _logger.error('%s bad DQ2 response for %s' % (self.token,dataset))
        # return
        return True


    # check rev to avoid too many rebrokerage
    def checkRev(self):
        # check specialHandling
        if self.job.specialHandling in [None,'NULL','']:
            revNum = 0
        else:
            revNum = self.job.specialHandling.split(',').count('rebro')
            revNum += self.job.specialHandling.split(',').count('sretry')
        # check with limit
        if revNum < 5:
            return True
        return False
                                                                                                                                    

    # make buildJob for re-brokerage
    def makeNewBuildJobForRebrokerage(self,buildJob):
        # new libDS
        oldLibDS = buildJob.destinationDBlock
        match = re.search('_rev(\d+)$',oldLibDS)
        if match == None:
            newLibDS = oldLibDS + '__id%s_rev%s' % (self.job.jobDefinitionID,self.revNum)
        else:
            newLibDS = re.sub('_rev(\d+)$','_rev%s' % self.revNum,oldLibDS)
        # reset parameters
        buildJob.PandaID           = None
        buildJob.jobStatus         = None
        buildJob.commandToPilot    = None
        buildJob.schedulerID       = None
        buildJob.pilotID           = None
        for attr in buildJob._attributes:
            if attr.endswith('ErrorCode') or attr.endswith('ErrorDiag'):
                setattr(buildJob,attr,None)
        buildJob.transExitCode     = None
        buildJob.creationTime      = datetime.datetime.utcnow()
        buildJob.modificationTime  = buildJob.creationTime
        buildJob.startTime         = None
        buildJob.endTime           = None
        buildJob.destinationDBlock = newLibDS
        buildJob.jobParameters = re.sub(oldLibDS,newLibDS,buildJob.jobParameters)
        for tmpFile in buildJob.Files:
            tmpFile.row_ID  = None
            tmpFile.GUID    = None
            tmpFile.status  = 'unknown'
            tmpFile.PandaID = None
            tmpFile.dataset = newLibDS
            tmpFile.destinationDBlock = tmpFile.dataset
            tmpFile.lfn = re.sub(oldLibDS,newLibDS,tmpFile.lfn)
        return buildJob,oldLibDS,newLibDS
