import os
import re
import sys
import time
import signal
import socket
import commands
import optparse
import datetime
import cPickle as pickle

from dq2.common import log as logging
from dq2.common import stomp
from config import panda_config
from brokerage.SiteMapper import SiteMapper
from dataservice.Finisher import Finisher
from dataservice import DataServiceUtils


# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('fileCallbackListener')

# keep PID
pidFile = '%s/file_callback_listener.pid' % panda_config.logdir

# overall timeout value
overallTimeout = 60 * 59

# expiration time
expirationTime = datetime.datetime.utcnow() + datetime.timedelta(minutes=overallTimeout)


# kill whole process
def catch_sig(sig, frame):
    try:
        os.remove(pidFile)
    except:
        pass
    # kill
    _logger.debug('terminating ...')
    commands.getoutput('kill -9 -- -%s' % os.getpgrp())
    # exit
    sys.exit(0)
                                        

# callback listener
class FileCallbackListener(stomp.ConnectionListener):

    def __init__(self,conn,tb,sm):
        # connection
        self.conn = conn
        # task buffer
        self.taskBuffer = tb
        # site mapper
        self.siteMapper = sm

        
    def on_error(self,headers,body):
        _logger.error("on_error : %s" % headers['message'])


    def on_disconnected(self,headers,body):
        _logger.error("on_disconnected : %s" % headers['message'])
                        

    def on_message(self, headers, message):
        try:
            lfn = 'UNKNOWN'
            # send ack
            id = headers['message-id']
            self.conn.ack({'message-id':id})
            # check message type
            messageType = headers['cbtype']
            if not messageType in ['FileDoneMessage']:
                _logger.debug('%s skip' % messageType)
                return
            _logger.debug('%s start' % messageType)            
            # re-construct message
            messageObj = pickle.loads(message)
            evtTime = datetime.datetime.utcfromtimestamp(messageObj.getItem('eventTime'))
            lfn     = messageObj.getItem('lfn')
            guid    = messageObj.getItem('guid')
            ddmSite = messageObj.getItem('site')
            _logger.debug('%s site=%s type=%s time=%s' % \
                          (lfn,ddmSite,messageType,evtTime.strftime('%Y-%m-%d %H:%M:%S')))
            # ignore non production files
            flagNgPrefix = False
            for ngPrefix in ['user','step']:
                if lfn.startswith(ngPrefix):
                    flagNgPrefix = True
                    break
            if flagNgPrefix:
                _logger.debug('%s skip' % lfn)                
                return
            # get datasets associated with the file only for high priority jobs
            dsNameMap = self.taskBuffer.getDatasetWithFile(lfn,800)
            _logger.debug('%s ds=%s' % (lfn,str(dsNameMap)))
            # loop over all datasets
            for dsName,dsData in dsNameMap.iteritems():
                pandaSite,dsToken = dsData
                # skip multiple destination since each file doesn't have
                # transferStatus
                if not dsToken in ['',None] and ',' in dsToken:
                    _logger.debug('%s ignore ds=%s token=%s' % (lfn,dsName,dsToken))
                    continue
                # check site
                tmpSiteSpec = self.siteMapper.getSite(pandaSite)
                if tmpSiteSpec.setokens.has_key(dsToken):
                    pandaSiteDdmID = tmpSiteSpec.setokens[dsToken]
                else:
                    pandaSiteDdmID = tmpSiteSpec.ddm
                if  pandaSiteDdmID != ddmSite:
                    _logger.debug('%s ignore ds=%s site=%s:%s <> %s' % \
                                  (lfn,dsName,pandaSite,pandaSiteDdmID,ddmSite))
                    continue
                # update file
                forInput = None
                if re.search('_dis\d+$',dsName) != None:
                    # dispatch datasets
                    forInput = True
                    ids = self.taskBuffer.updateInFilesReturnPandaIDs(dsName,'ready',lfn)
                elif re.search('_sub\d+$',dsName) != None:
                    # sub datasets
                    forInput = False
                    ids = self.taskBuffer.updateOutFilesReturnPandaIDs(dsName,lfn)
                _logger.debug('%s ds=%s ids=%s' % (lfn,dsName,str(ids)))
                # loop over all PandaIDs
                if forInput != None and len(ids) != 0:
                    # remove None and unknown
                    targetIDs = []
                    for tmpID in ids:
                        # count the number of pending files
                        nPending = self.taskBuffer.countPendingFiles(tmpID,forInput)
                        _logger.debug('%s PandaID=%s nPen=%s' % (lfn,tmpID,nPending))
                        if nPending != 0:
                            continue
                        targetIDs.append(tmpID)
                    # get jobs
                    targetJobs = []
                    if targetIDs != []:
                        if forInput:
                            jobs = self.taskBuffer.peekJobs(targetIDs,fromActive=False,fromArchived=False,
                                                            fromWaiting=False)
                        else:
                            jobs = self.taskBuffer.peekJobs(targetIDs,fromDefined=False,fromArchived=False,
                                                            fromWaiting=False)
                        for tmpJob in jobs:
                            if tmpJob == None or tmpJob.jobStatus == 'unknown':
                                continue
                            targetJobs.append(tmpJob)
                    # trigger subsequent processe
                    if targetJobs == []:
                        _logger.debug('%s no jobs to be triggerd for subsequent processe' % lfn)
                    else:
                        if forInput:
                            # activate
                            _logger.debug('%s activate %s' % (lfn,str(targetIDs)))
                            self.taskBuffer.activateJobs(targetJobs)
                        else:
                            # finish
                            _logger.debug('%s finish %s' % (lfn,str(targetIDs)))                        
                            for tmpJob in targetJobs:
                                fThr = Finisher(self.taskBuffer,None,tmpJob)
                                fThr.start()
                                fThr.join()
            _logger.debug('%s done' % lfn)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("on_message : %s %s %s" % (lfn,errtype,errvalue))
        

# main
def main(backGround=False): 
    _logger.debug('starting ...')
    # register signal handler
    signal.signal(signal.SIGINT, catch_sig)
    signal.signal(signal.SIGHUP, catch_sig)
    signal.signal(signal.SIGTERM,catch_sig)
    signal.signal(signal.SIGALRM,catch_sig)
    signal.alarm(overallTimeout)
    # forking    
    pid = os.fork()
    if pid != 0:
        # watch child process
        os.wait()
        time.sleep(1)
    else:    
        # main loop
        from taskbuffer.TaskBuffer import taskBuffer
        # check certificate
        certName = '/data/atlpan/pandasv1_usercert.pem'
        _logger.debug('checking certificate {0}'.format(certName))
        certOK,certMsg = DataServiceUtils.checkCertificate(certName)
        if not certOK:
            _logger.error('bad certificate : {0}'.format(certMsg))
        # initialize cx_Oracle using dummy connection
        from taskbuffer.Initializer import initializer
        initializer.init()
        # instantiate TB
        taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)
        # instantiate sitemapper
        siteMapper = SiteMapper(taskBuffer)
        # ActiveMQ params
        clientid = 'PANDA-' + socket.getfqdn()
        queue = '/queue/Consumer.PANDA.atlas.ddm.siteservices'
        ssl_opts = {'use_ssl' : True,
                    'ssl_cert_file' : certName,
                    'ssl_key_file'  : '/data/atlpan/pandasv1_userkey.pem'}
        # resolve multiple brokers
        brokerList = socket.gethostbyname_ex('atlasddm-mb.cern.ch')[-1]
        # set listener
        for tmpBroker in brokerList:
            try:
                _logger.debug('setting listener on %s' % tmpBroker)                
                conn = stomp.Connection(host_and_ports = [(tmpBroker, 6162)], **ssl_opts)
                conn.set_listener('FileCallbackListener', FileCallbackListener(conn,taskBuffer,siteMapper))
                conn.start()
                conn.connect(headers = {'client-id': clientid})
                conn.subscribe(destination=queue, ack='client-individual')
                               #,headers = {'selector':"cbtype='FileDoneMessage'"})
                if not conn.is_connected():
                    _logger.error("connection failure to %s" % tmpBroker)
            except:     
                errtype,errvalue = sys.exc_info()[:2]
                _logger.error("failed to set listener on %s : %s %s" % (tmpBroker,errtype,errvalue))
                catch_sig(None,None)
            
# entry
if __name__ == "__main__":
    optP = optparse.OptionParser(conflict_handler="resolve")
    options,args = optP.parse_args()
    try:
        # time limit
        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(seconds=overallTimeout-180)
        # get process list
        scriptName = sys.argv[0]
        out = commands.getoutput('env TZ=UTC ps axo user,pid,lstart,args | grep %s' % scriptName)
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
        errtype,errvalue = sys.exc_info()[:2]
        _logger.error("kill process : %s %s" % (errtype,errvalue))
    # main loop    
    main()
