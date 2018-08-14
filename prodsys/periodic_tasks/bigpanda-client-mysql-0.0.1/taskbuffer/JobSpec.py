"""
job specification

"""

class JobSpec(object):
    # attributes
    _attributes = ('PandaID','jobDefinitionID','schedulerID','pilotID','creationTime','creationHost',
                   'modificationTime','modificationHost','AtlasRelease','transformation','homepackage',
                   'prodSeriesLabel','prodSourceLabel','prodUserID','assignedPriority','currentPriority',
                   'attemptNr','maxAttempt','jobStatus','jobName','maxCpuCount','maxCpuUnit','maxDiskCount',
                   'maxDiskUnit','ipConnectivity','minRamCount','minRamUnit','startTime','endTime',
                   'cpuConsumptionTime','cpuConsumptionUnit','commandToPilot','transExitCode','pilotErrorCode',
                   'pilotErrorDiag','exeErrorCode','exeErrorDiag','supErrorCode','supErrorDiag',
                   'ddmErrorCode','ddmErrorDiag','brokerageErrorCode','brokerageErrorDiag',
                   'jobDispatcherErrorCode','jobDispatcherErrorDiag','taskBufferErrorCode',
                   'taskBufferErrorDiag','computingSite','computingElement','jobParameters',
                   'metadata','prodDBlock','dispatchDBlock','destinationDBlock','destinationSE',
                   'nEvents','grid','cloud','cpuConversion','sourceSite','destinationSite','transferType',
                   'taskID','cmtConfig','stateChangeTime','prodDBUpdateTime','lockedby','relocationFlag',
                   'jobExecutionID','VO','pilotTiming','workingGroup','processingType','prodUserName',
                   'nInputFiles','countryGroup','batchID','parentID','specialHandling','jobsetID',
                   'coreCount','nInputDataFiles','inputFileType','inputFileProject','inputFileBytes',
                   'nOutputDataFiles','outputFileBytes','jobMetrics','workQueue_ID','jediTaskID')
    # slots
    __slots__ = _attributes+('Files','_changedAttrs')
    # attributes which have 0 by default
    _zeroAttrs = ('assignedPriority','currentPriority','attemptNr','maxAttempt','maxCpuCount','maxDiskCount',
                  'minRamCount','cpuConsumptionTime','pilotErrorCode','exeErrorCode','supErrorCode','ddmErrorCode',
                  'brokerageErrorCode','jobDispatcherErrorCode','taskBufferErrorCode','nEvents','relocationFlag',
                  'jobExecutionID','nOutputDataFiles','outputFileBytes')
    # attribute to be suppressed. They are in another table
    _suppAttrs = ('jobParameters','metadata')
    # mapping between sequence and attr
    _seqAttrMap = {'PandaID':'ATLAS_PANDA.JOBSDEFINED4_PANDAID_SEQ.nextval'}
    # limit length
    _limitLength = {'ddmErrorDiag'           : 500,
                    'taskBufferErrorDiag'    : 300,
                    'jobDispatcherErrorDiag' : 250,
                    'brokerageErrorDiag'     : 250,
                    'pilotErrorDiag'         : 500,
                    'exeErrorDiag'           : 500,
                    }


    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            object.__setattr__(self,attr,None)
        # files list
        object.__setattr__(self,'Files',[])
        # map of changed attributes
        object.__setattr__(self,'_changedAttrs',{})
        

    # override __getattribute__ for SQL
    def __getattribute__(self,name):
        ret = object.__getattribute__(self,name)
        if ret == None:
            return "NULL"
        return ret


    # override __setattr__ to collecte the changed attributes
    def __setattr__(self,name,value):
        oldVal = getattr(self,name)
        object.__setattr__(self,name,value)
        newVal = getattr(self,name)
        # collect changed attributes
        if oldVal != newVal and not name in self._suppAttrs:
            self._changedAttrs[name] = value

            
    # reset changed attribute list
    def resetChangedList(self):
        object.__setattr__(self,'_changedAttrs',{})

        
    # add File to files list
    def addFile(self,file):
        # set owner
        file.setOwner(self)
        # append
        self.Files.append(file)
        
    
    # pack tuple into JobSpec
    def pack(self,values):
        for i in range(len(self._attributes)):
            attr= self._attributes[i]
            val = values[i]
            object.__setattr__(self,attr,val)


    # return a tuple of values
    def values(self):
        ret = []
        for attr in self._attributes:
            val = getattr(self,attr)
            ret.append(val)
        return tuple(ret)


    # return map of values
    def valuesMap(self,useSeq=False,onlyChanged=False):
        ret = {}
        for attr in self._attributes:
            if useSeq and self._seqAttrMap.has_key(attr):
                continue
            if onlyChanged:
                if not self._changedAttrs.has_key(attr):
                    continue
            val = getattr(self,attr)
            if val == 'NULL':
                if attr in self._zeroAttrs:
                    val = 0
                else:
                    val = None
            # jobParameters/metadata go to another table
            if attr in self._suppAttrs:
                val = None
            # truncate too long values
            if self._limitLength.has_key(attr):
                if val != None:
                    val = val[:self._limitLength[attr]]
            ret[':%s' % attr] = val
        return ret


    # return state values to be pickled
    def __getstate__(self):
        state = []
        for attr in self._attributes:
            val = getattr(self,attr)
            state.append(val)
        # append File info
        state.append(self.Files)
        return state


    # restore state from the unpickled state values
    def __setstate__(self,state):
        for i in range(len(self._attributes)):
            # schema evolution is supported only when adding attributes
            if i+1 < len(state):
                object.__setattr__(self,self._attributes[i],state[i])
            else:
                object.__setattr__(self,self._attributes[i],'NULL')                
        object.__setattr__(self,'Files',state[-1])
        object.__setattr__(self,'_changedAttrs',{})
        
        
    # return column names for INSERT or full SELECT
    def columnNames(cls):
        ret = ""
        for attr in cls._attributes:
            if ret != "":
                ret += ','
            ret += attr
        return ret
    columnNames = classmethod(columnNames)


    # return expression of values for INSERT
    def valuesExpression(cls):
        ret = "VALUES("
        for attr in cls._attributes:
            ret += "%s"
            if attr != cls._attributes[len(cls._attributes)-1]:
                ret += ","
        ret += ")"            
        return ret
    valuesExpression = classmethod(valuesExpression)


    # return expression of bind values for INSERT
    def bindValuesExpression(cls, useSeq=False, backend='oracle'):
        ret = "VALUES("
        for attr in cls._attributes:
            if useSeq and cls._seqAttrMap.has_key(attr):
                if backend == 'mysql':
                    ret += "%s," % "NULL"
                else:  # backend == 'oracle'
                    ret += "%s," % cls._seqAttrMap[attr]
            else:
                ret += ":%s," % attr
        ret = ret[:-1]
        ret += ")"
        return ret
    bindValuesExpression = classmethod(bindValuesExpression)


    # return an expression for UPDATE
    def updateExpression(cls):
        ret = ""
        for attr in cls._attributes:
            ret = ret + attr + "=%s"
            if attr != cls._attributes[len(cls._attributes)-1]:
                ret += ","
        return ret
    updateExpression = classmethod(updateExpression)


    # return an expression of bind variables for UPDATE
    def bindUpdateExpression(cls):
        ret = ""
        for attr in cls._attributes:
            ret += '%s=:%s,' % (attr,attr)
        ret = ret[:-1]
        ret += ' '        
        return ret
    bindUpdateExpression = classmethod(bindUpdateExpression)


    # comparison function for sort
    def compFunc(cls,a,b):
        iPandaID  = list(cls._attributes).index('PandaID')
        iPriority = list(cls._attributes).index('currentPriority')
        if a[iPriority] > b[iPriority]:
            return -1
        elif a[iPriority] < b[iPriority]:
            return 1
        else:
            if a[iPandaID] > b[iPandaID]:
                return 1
            elif a[iPandaID] < b[iPandaID]:
                return -1
            else:
                return 0
    compFunc = classmethod(compFunc)


    # return an expression of bind variables for UPDATE to update only changed attributes
    def bindUpdateChangesExpression(self):
        ret = ""
        for attr in self._attributes:
            if self._changedAttrs.has_key(attr):
                ret += '%s=:%s,' % (attr,attr)
        ret  = ret[:-1]
        ret += ' '
        return ret
