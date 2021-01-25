# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models

# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey has `on_delete` set to the desired behavior.
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.

class Schedconfig(models.Model):
    name = models.CharField(db_column='NAME', max_length=60)  # Field name made lowercase.

    def __str__(self):
        return self.name

    nickname = models.CharField(db_column='NICKNAME', primary_key=True, max_length=60)  # Field name made lowercase.
    queue = models.CharField(db_column='QUEUE', max_length=60, blank=True, null=True)  # Field name made lowercase.
    localqueue = models.CharField(db_column='LOCALQUEUE', max_length=50, blank=True, null=True)  # Field name made lowercase.
    system = models.CharField(db_column='SYSTEM', max_length=60)  # Field name made lowercase.
    sysconfig = models.CharField(db_column='SYSCONFIG', max_length=20, blank=True, null=True)  # Field name made lowercase.
    environ = models.CharField(db_column='ENVIRON', max_length=250, blank=True, null=True)  # Field name made lowercase.
    gatekeeper = models.CharField(db_column='GATEKEEPER', max_length=120, blank=True, null=True)  # Field name made lowercase.
    jobmanager = models.CharField(db_column='JOBMANAGER', max_length=80, blank=True, null=True)  # Field name made lowercase.
    se = models.CharField(db_column='SE', max_length=400, blank=True, null=True)  # Field name made lowercase.
    ddm = models.CharField(db_column='DDM', max_length=120, blank=True, null=True)  # Field name made lowercase.
    jdladd = models.CharField(db_column='JDLADD', max_length=500, blank=True, null=True)  # Field name made lowercase.
    globusadd = models.CharField(db_column='GLOBUSADD', max_length=100, blank=True, null=True)  # Field name made lowercase.
    jdl = models.CharField(db_column='JDL', max_length=60, blank=True, null=True)  # Field name made lowercase.
    jdltxt = models.CharField(db_column='JDLTXT', max_length=500, blank=True, null=True)  # Field name made lowercase.
    version = models.CharField(db_column='VERSION', max_length=60, blank=True, null=True)  # Field name made lowercase.
    site = models.CharField(db_column='SITE', max_length=60)  # Field name made lowercase.
    region = models.CharField(db_column='REGION', max_length=60, blank=True, null=True)  # Field name made lowercase.
    gstat = models.CharField(db_column='GSTAT', max_length=60, blank=True, null=True)  # Field name made lowercase.
    tags = models.CharField(db_column='TAGS', max_length=200, blank=True, null=True)  # Field name made lowercase.
    cmd = models.CharField(db_column='CMD', max_length=200, blank=True, null=True)  # Field name made lowercase.
    lastmod = models.DateTimeField(db_column='LASTMOD')  # Field name made lowercase.
    errinfo = models.CharField(db_column='ERRINFO', max_length=80, blank=True, null=True)  # Field name made lowercase.
    nqueue = models.IntegerField(db_column='NQUEUE')  # Field name made lowercase.
    comment_field = models.CharField(db_column='COMMENT_', max_length=500, blank=True, null=True)  # Field name made lowercase. Field renamed because it ended with '_'.
    appdir = models.CharField(db_column='APPDIR', max_length=500, blank=True, null=True)  # Field name made lowercase.
    datadir = models.CharField(db_column='DATADIR', max_length=80, blank=True, null=True)  # Field name made lowercase.
    tmpdir = models.CharField(db_column='TMPDIR', max_length=80, blank=True, null=True)  # Field name made lowercase.
    wntmpdir = models.CharField(db_column='WNTMPDIR', max_length=80, blank=True, null=True)  # Field name made lowercase.
    dq2url = models.CharField(db_column='DQ2URL', max_length=80, blank=True, null=True)  # Field name made lowercase.
    special_par = models.CharField(db_column='SPECIAL_PAR', max_length=80, blank=True, null=True)  # Field name made lowercase.
    python_path = models.CharField(db_column='PYTHON_PATH', max_length=80, blank=True, null=True)  # Field name made lowercase.
    nodes = models.IntegerField(db_column='NODES')  # Field name made lowercase.
    status = models.CharField(db_column='STATUS', max_length=10, blank=True, null=True)  # Field name made lowercase.
    copytool = models.CharField(db_column='COPYTOOL', max_length=80, blank=True, null=True)  # Field name made lowercase.
    copysetup = models.CharField(db_column='COPYSETUP', max_length=200, blank=True, null=True)  # Field name made lowercase.
    releases = models.CharField(db_column='RELEASES', max_length=500, blank=True, null=True)  # Field name made lowercase.
    sepath = models.CharField(db_column='SEPATH', max_length=400, blank=True, null=True)  # Field name made lowercase.
    envsetup = models.CharField(db_column='ENVSETUP', max_length=200, blank=True, null=True)  # Field name made lowercase.
    copyprefix = models.CharField(db_column='COPYPREFIX', max_length=160, blank=True, null=True)  # Field name made lowercase.
    lfcpath = models.CharField(db_column='LFCPATH', max_length=80, blank=True, null=True)  # Field name made lowercase.
    seopt = models.CharField(db_column='SEOPT', max_length=400, blank=True, null=True)  # Field name made lowercase.
    sein = models.CharField(db_column='SEIN', max_length=400, blank=True, null=True)  # Field name made lowercase.
    seinopt = models.CharField(db_column='SEINOPT', max_length=400, blank=True, null=True)  # Field name made lowercase.
    lfchost = models.CharField(db_column='LFCHOST', max_length=80, blank=True, null=True)  # Field name made lowercase.
    cloud = models.CharField(db_column='CLOUD', max_length=60, blank=True, null=True)  # Field name made lowercase.
    siteid = models.CharField(db_column='SITEID', max_length=60, blank=True, null=True)  # Field name made lowercase.
    proxy = models.CharField(db_column='PROXY', max_length=80, blank=True, null=True)  # Field name made lowercase.
    retry = models.CharField(db_column='RETRY', max_length=10, blank=True, null=True)  # Field name made lowercase.
    queuehours = models.IntegerField(db_column='QUEUEHOURS')  # Field name made lowercase.
    envsetupin = models.CharField(db_column='ENVSETUPIN', max_length=200, blank=True, null=True)  # Field name made lowercase.
    copytoolin = models.CharField(db_column='COPYTOOLIN', max_length=180, blank=True, null=True)  # Field name made lowercase.
    copysetupin = models.CharField(db_column='COPYSETUPIN', max_length=200, blank=True, null=True)  # Field name made lowercase.
    seprodpath = models.CharField(db_column='SEPRODPATH', max_length=400, blank=True, null=True)  # Field name made lowercase.
    lfcprodpath = models.CharField(db_column='LFCPRODPATH', max_length=80, blank=True, null=True)  # Field name made lowercase.
    copyprefixin = models.CharField(db_column='COPYPREFIXIN', max_length=360, blank=True, null=True)  # Field name made lowercase.
    recoverdir = models.CharField(db_column='RECOVERDIR', max_length=80, blank=True, null=True)  # Field name made lowercase.
    memory = models.IntegerField(db_column='MEMORY')  # Field name made lowercase.
    maxtime = models.IntegerField(db_column='MAXTIME')  # Field name made lowercase.
    space = models.IntegerField(db_column='SPACE')  # Field name made lowercase.
    tspace = models.DateTimeField(db_column='TSPACE')  # Field name made lowercase.
    cmtconfig = models.CharField(db_column='CMTCONFIG', max_length=250, blank=True, null=True)  # Field name made lowercase.
    setokens = models.CharField(db_column='SETOKENS', max_length=80, blank=True, null=True)  # Field name made lowercase.
    glexec = models.CharField(db_column='GLEXEC', max_length=10, blank=True, null=True)  # Field name made lowercase.
    priorityoffset = models.CharField(db_column='PRIORITYOFFSET', max_length=60, blank=True, null=True)  # Field name made lowercase.
    allowedgroups = models.CharField(db_column='ALLOWEDGROUPS', max_length=100, blank=True, null=True)  # Field name made lowercase.
    defaulttoken = models.CharField(db_column='DEFAULTTOKEN', max_length=100, blank=True, null=True)  # Field name made lowercase.
    pcache = models.CharField(db_column='PCACHE', max_length=100, blank=True, null=True)  # Field name made lowercase.
    validatedreleases = models.CharField(db_column='VALIDATEDRELEASES', max_length=500, blank=True, null=True)  # Field name made lowercase.
    accesscontrol = models.CharField(db_column='ACCESSCONTROL', max_length=20, blank=True, null=True)  # Field name made lowercase.
    dn = models.CharField(db_column='DN', max_length=100, blank=True, null=True)  # Field name made lowercase.
    email = models.CharField(db_column='EMAIL', max_length=60, blank=True, null=True)  # Field name made lowercase.
    allowednode = models.CharField(db_column='ALLOWEDNODE', max_length=80, blank=True, null=True)  # Field name made lowercase.
    maxinputsize = models.IntegerField(db_column='MAXINPUTSIZE', blank=True, null=True)  # Field name made lowercase.
    timefloor = models.IntegerField(db_column='TIMEFLOOR', blank=True, null=True)  # Field name made lowercase.
    depthboost = models.IntegerField(db_column='DEPTHBOOST', blank=True, null=True)  # Field name made lowercase.
    idlepilotsupression = models.IntegerField(db_column='IDLEPILOTSUPRESSION', blank=True, null=True)  # Field name made lowercase.
    pilotlimit = models.IntegerField(db_column='PILOTLIMIT', blank=True, null=True)  # Field name made lowercase.
    transferringlimit = models.IntegerField(db_column='TRANSFERRINGLIMIT', blank=True, null=True)  # Field name made lowercase.
    cachedse = models.IntegerField(db_column='CACHEDSE', blank=True, null=True)  # Field name made lowercase.
    corecount = models.IntegerField(db_column='CORECOUNT', blank=True, null=True)  # Field name made lowercase.
    countrygroup = models.CharField(db_column='COUNTRYGROUP', max_length=64, blank=True, null=True)  # Field name made lowercase.
    availablecpu = models.CharField(db_column='AVAILABLECPU', max_length=64, blank=True, null=True)  # Field name made lowercase.
    availablestorage = models.CharField(db_column='AVAILABLESTORAGE', max_length=64, blank=True, null=True)  # Field name made lowercase.
    pledgedcpu = models.CharField(db_column='PLEDGEDCPU', max_length=64, blank=True, null=True)  # Field name made lowercase.
    pledgedstorage = models.CharField(db_column='PLEDGEDSTORAGE', max_length=64, blank=True, null=True)  # Field name made lowercase.
    statusoverride = models.CharField(db_column='STATUSOVERRIDE', max_length=256, blank=True, null=True)  # Field name made lowercase.
    allowdirectaccess = models.CharField(db_column='ALLOWDIRECTACCESS', max_length=10, blank=True, null=True)  # Field name made lowercase.
    gocname = models.CharField(db_column='GOCNAME', max_length=64, blank=True, null=True)  # Field name made lowercase.
    tier = models.CharField(db_column='TIER', max_length=15, blank=True, null=True)  # Field name made lowercase.
    multicloud = models.CharField(db_column='MULTICLOUD', max_length=64, blank=True, null=True)  # Field name made lowercase.
    lfcregister = models.CharField(db_column='LFCREGISTER', max_length=10, blank=True, null=True)  # Field name made lowercase.
    stageinretry = models.IntegerField(db_column='STAGEINRETRY', blank=True, null=True)  # Field name made lowercase.
    stageoutretry = models.IntegerField(db_column='STAGEOUTRETRY', blank=True, null=True)  # Field name made lowercase.
    fairsharepolicy = models.CharField(db_column='FAIRSHAREPOLICY', max_length=512, blank=True, null=True)  # Field name made lowercase.
    allowfax = models.CharField(db_column='ALLOWFAX', max_length=64, blank=True, null=True)  # Field name made lowercase.
    faxredirector = models.CharField(db_column='FAXREDIRECTOR', max_length=256, blank=True, null=True)  # Field name made lowercase.
    maxwdir = models.IntegerField(db_column='MAXWDIR', blank=True, null=True)  # Field name made lowercase.
    celist = models.CharField(db_column='CELIST', max_length=4000, blank=True, null=True)  # Field name made lowercase.
    minmemory = models.IntegerField(db_column='MINMEMORY', blank=True, null=True)  # Field name made lowercase.
    maxmemory = models.IntegerField(db_column='MAXMEMORY', blank=True, null=True)  # Field name made lowercase.
    mintime = models.IntegerField(db_column='MINTIME', blank=True, null=True)  # Field name made lowercase.
    allowjem = models.CharField(db_column='ALLOWJEM', max_length=64, blank=True, null=True)  # Field name made lowercase.
    catchall = models.CharField(db_column='CATCHALL', max_length=512, blank=True, null=True)  # Field name made lowercase.
    faxdoor = models.CharField(db_column='FAXDOOR', max_length=128, blank=True, null=True)  # Field name made lowercase.
    wansourcelimit = models.IntegerField(db_column='WANSOURCELIMIT', blank=True, null=True)  # Field name made lowercase.
    wansinklimit = models.IntegerField(db_column='WANSINKLIMIT', blank=True, null=True)  # Field name made lowercase.
    auto_mcu = models.IntegerField(db_column='AUTO_MCU')  # Field name made lowercase.
    objectstore = models.CharField(db_column='OBJECTSTORE', max_length=512, blank=True, null=True)  # Field name made lowercase.
    allowhttp = models.CharField(db_column='ALLOWHTTP', max_length=64, blank=True, null=True)  # Field name made lowercase.
    httpredirector = models.CharField(db_column='HTTPREDIRECTOR', max_length=256, blank=True, null=True)  # Field name made lowercase.
    multicloud_append = models.CharField(db_column='MULTICLOUD_APPEND', max_length=64, blank=True, null=True)  # Field name made lowercase.
#    corepower = models.IntegerField(db_column='COREPOWER', blank=True, null=True)  # Field name made lowercase.
#    wnconnectivity = models.CharField(db_column='WNCONNECTIVITY', max_length=256, blank=True, null=True)  # Field name made lowercase.
#    cloudrshare = models.CharField(db_column='CLOUDRSHARE', max_length=256, blank=True, null=True)  # Field name made lowercase.
#    sitershare = models.CharField(db_column='SITERSHARE', max_length=256, blank=True, null=True)  # Field name made lowercase.
#    autosetup_post = models.CharField(db_column='AUTOSETUP_POST', max_length=256, blank=True, null=True)  # Field name made lowercase.
#    autosetup_pre = models.CharField(db_column='AUTOSETUP_PRE', max_length=256, blank=True, null=True)  # Field name made lowercase.
#    direct_access_lan = models.CharField(db_column='DIRECT_ACCESS_LAN', max_length=256, blank=True, null=True)  # Field name made lowercase.
#    direct_access_wan = models.CharField(db_column='DIRECT_ACCESS_WAN', max_length=256, blank=True, null=True)  # Field name made lowercase.
#    maxrss = models.IntegerField(db_column='MAXRSS', blank=True, null=True)  # Field name made lowercase.
#    minrss = models.IntegerField(db_column='MINRSS', blank=True, null=True)  # Field name made lowercase.
#    use_newmover = models.CharField(db_column='USE_NEWMOVER', max_length=32, blank=True, null=True)  # Field name made lowercase.
#    pilotversion = models.CharField(db_column='PILOTVERSION', max_length=32, blank=True, null=True)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'schedconfig'
        verbose_name_plural = 'schedconfig'


class Cloudconfig(models.Model):
    name = models.CharField(db_column='NAME', primary_key=True, max_length=20)  # Field name made lowercase.

    def __str__(self):
        return self.name

    description = models.CharField(db_column='DESCRIPTION', max_length=50)  # Field name made lowercase.
    tier1 = models.CharField(db_column='TIER1', max_length=20)  # Field name made lowercase.
    tier1se = models.CharField(db_column='TIER1SE', max_length=400)  # Field name made lowercase.
    relocation = models.CharField(db_column='RELOCATION', max_length=10, blank=True, null=True)  # Field name made lowercase.
    weight = models.IntegerField(db_column='WEIGHT')  # Field name made lowercase.
    server = models.CharField(db_column='SERVER', max_length=100)  # Field name made lowercase.
    status = models.CharField(db_column='STATUS', max_length=20)  # Field name made lowercase.
    transtimelo = models.IntegerField(db_column='TRANSTIMELO')  # Field name made lowercase.
    transtimehi = models.IntegerField(db_column='TRANSTIMEHI')  # Field name made lowercase.
    waittime = models.IntegerField(db_column='WAITTIME')  # Field name made lowercase.
    comment_field = models.CharField(db_column='COMMENT_', max_length=200, blank=True, null=True)  # Field name made lowercase. Field renamed because it ended with '_'.
    space = models.IntegerField(db_column='SPACE')  # Field name made lowercase.
    moduser = models.CharField(db_column='MODUSER', max_length=30, blank=True, null=True)  # Field name made lowercase.
    modtime = models.DateTimeField(db_column='MODTIME')  # Field name made lowercase.
    validation = models.CharField(db_column='VALIDATION', max_length=20, blank=True, null=True)  # Field name made lowercase.
    mcshare = models.IntegerField(db_column='MCSHARE')  # Field name made lowercase.
    countries = models.CharField(db_column='COUNTRIES', max_length=80, blank=True, null=True)  # Field name made lowercase.
    fasttrack = models.CharField(db_column='FASTTRACK', max_length=20, blank=True, null=True)  # Field name made lowercase.
    nprestage = models.IntegerField(db_column='NPRESTAGE')  # Field name made lowercase.
    pilotowners = models.CharField(db_column='PILOTOWNERS', max_length=300, blank=True, null=True)  # Field name made lowercase.
    dn = models.CharField(db_column='DN', max_length=100, blank=True, null=True)  # Field name made lowercase.
    email = models.CharField(db_column='EMAIL', max_length=60, blank=True, null=True)  # Field name made lowercase.
    fairshare = models.CharField(db_column='FAIRSHARE', max_length=128, blank=True, null=True)  # Field name made lowercase.
    auto_mcu = models.IntegerField(db_column='AUTO_MCU')  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'cloudconfig'
        verbose_name_plural = 'cloudconfig'



class Siteaccess(models.Model):
    id = models.AutoField(db_column='ID', primary_key=True)  # Field name made lowercase.
    dn = models.CharField(db_column='DN', max_length=100, blank=True, null=True)  # Field name made lowercase.
    pandasite = models.CharField(db_column='PANDASITE', max_length=100, blank=True, null=True)  # Field name made lowercase.

    def __str__(self):
        return self.pandasite

    poffset = models.IntegerField(db_column='POFFSET')  # Field name made lowercase.
    rights = models.CharField(db_column='RIGHTS', max_length=30, blank=True, null=True)  # Field name made lowercase.
    status = models.CharField(db_column='STATUS', max_length=20, blank=True, null=True)  # Field name made lowercase.
    workinggroups = models.CharField(db_column='WORKINGGROUPS', max_length=100, blank=True, null=True)  # Field name made lowercase.
    created = models.DateField(db_column='CREATED', blank=True, null=True)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'siteaccess'
        verbose_name_plural = "siteaccess"
        unique_together = (('dn', 'pandasite'),)


class Users(models.Model):
    id = models.IntegerField(db_column='ID', primary_key=True)  # Field name made lowercase.
    name = models.CharField(db_column='NAME', max_length=60)  # Field name made lowercase.

    def __str__(self):
        return self.name

    dn = models.CharField(db_column='DN', max_length=150, blank=True, null=True)  # Field name made lowercase.
    email = models.CharField(db_column='EMAIL', max_length=60, blank=True, null=True)  # Field name made lowercase.
    url = models.CharField(db_column='URL', max_length=100, blank=True, null=True)  # Field name made lowercase.
    location = models.CharField(db_column='LOCATION', max_length=60, blank=True, null=True)  # Field name made lowercase.
    classa = models.CharField(db_column='CLASSA', max_length=30, blank=True, null=True)  # Field name made lowercase.
    classp = models.CharField(db_column='CLASSP', max_length=30, blank=True, null=True)  # Field name made lowercase.
    classxp = models.CharField(db_column='CLASSXP', max_length=30, blank=True, null=True)  # Field name made lowercase.
    sitepref = models.CharField(db_column='SITEPREF', max_length=60, blank=True, null=True)  # Field name made lowercase.
    gridpref = models.CharField(db_column='GRIDPREF', max_length=20, blank=True, null=True)  # Field name made lowercase.
    queuepref = models.CharField(db_column='QUEUEPREF', max_length=60, blank=True, null=True)  # Field name made lowercase.
    scriptcache = models.CharField(db_column='SCRIPTCACHE', max_length=100, blank=True, null=True)  # Field name made lowercase.
    types = models.CharField(db_column='TYPES', max_length=60, blank=True, null=True)  # Field name made lowercase.
    sites = models.CharField(db_column='SITES', max_length=250, blank=True, null=True)  # Field name made lowercase.
    njobsa = models.IntegerField(db_column='NJOBSA', blank=True, null=True)  # Field name made lowercase.
    njobsp = models.IntegerField(db_column='NJOBSP', blank=True, null=True)  # Field name made lowercase.
    njobs1 = models.IntegerField(db_column='NJOBS1', blank=True, null=True)  # Field name made lowercase.
    njobs7 = models.IntegerField(db_column='NJOBS7', blank=True, null=True)  # Field name made lowercase.
    njobs30 = models.IntegerField(db_column='NJOBS30', blank=True, null=True)  # Field name made lowercase.
    cpua1 = models.BigIntegerField(db_column='CPUA1', blank=True, null=True)  # Field name made lowercase.
    cpua7 = models.BigIntegerField(db_column='CPUA7', blank=True, null=True)  # Field name made lowercase.
    cpua30 = models.BigIntegerField(db_column='CPUA30', blank=True, null=True)  # Field name made lowercase.
    cpup1 = models.BigIntegerField(db_column='CPUP1', blank=True, null=True)  # Field name made lowercase.
    cpup7 = models.BigIntegerField(db_column='CPUP7', blank=True, null=True)  # Field name made lowercase.
    cpup30 = models.BigIntegerField(db_column='CPUP30', blank=True, null=True)  # Field name made lowercase.
    cpuxp1 = models.BigIntegerField(db_column='CPUXP1', blank=True, null=True)  # Field name made lowercase.
    cpuxp7 = models.BigIntegerField(db_column='CPUXP7', blank=True, null=True)  # Field name made lowercase.
    cpuxp30 = models.BigIntegerField(db_column='CPUXP30', blank=True, null=True)  # Field name made lowercase.
    quotaa1 = models.BigIntegerField(db_column='QUOTAA1', blank=True, null=True)  # Field name made lowercase.
    quotaa7 = models.BigIntegerField(db_column='QUOTAA7', blank=True, null=True)  # Field name made lowercase.
    quotaa30 = models.BigIntegerField(db_column='QUOTAA30', blank=True, null=True)  # Field name made lowercase.
    quotap1 = models.BigIntegerField(db_column='QUOTAP1', blank=True, null=True)  # Field name made lowercase.
    quotap7 = models.BigIntegerField(db_column='QUOTAP7', blank=True, null=True)  # Field name made lowercase.
    quotap30 = models.BigIntegerField(db_column='QUOTAP30', blank=True, null=True)  # Field name made lowercase.
    quotaxp1 = models.BigIntegerField(db_column='QUOTAXP1', blank=True, null=True)  # Field name made lowercase.
    quotaxp7 = models.BigIntegerField(db_column='QUOTAXP7', blank=True, null=True)  # Field name made lowercase.
    quotaxp30 = models.BigIntegerField(db_column='QUOTAXP30', blank=True, null=True)  # Field name made lowercase.
    space1 = models.IntegerField(db_column='SPACE1', blank=True, null=True)  # Field name made lowercase.
    space7 = models.IntegerField(db_column='SPACE7', blank=True, null=True)  # Field name made lowercase.
    space30 = models.IntegerField(db_column='SPACE30', blank=True, null=True)  # Field name made lowercase.
    lastmod = models.DateTimeField(db_column='LASTMOD')  # Field name made lowercase.
    firstjob = models.DateTimeField(db_column='FIRSTJOB')  # Field name made lowercase.
    latestjob = models.DateTimeField(db_column='LATESTJOB')  # Field name made lowercase.
    pagecache = models.TextField(db_column='PAGECACHE', blank=True, null=True)  # Field name made lowercase.
    cachetime = models.DateTimeField(db_column='CACHETIME')  # Field name made lowercase.
    ncurrent = models.IntegerField(db_column='NCURRENT')  # Field name made lowercase.
    jobid = models.IntegerField(db_column='JOBID')  # Field name made lowercase.
    status = models.CharField(db_column='STATUS', max_length=20, blank=True, null=True)  # Field name made lowercase.
    vo = models.CharField(db_column='VO', max_length=20, blank=True, null=True)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'users'
        verbose_name_plural = 'users'

class PandaJob(models.Model):
    pandaid = models.BigIntegerField(primary_key=True, db_column='PANDAID') # Field name made lowercase.
    jobdefinitionid = models.BigIntegerField(db_column='JOBDEFINITIONID') # Field name made lowercase.
    schedulerid = models.CharField(max_length=384, db_column='SCHEDULERID', blank=True) # Field name made lowercase.
    pilotid = models.CharField(max_length=600, db_column='PILOTID', blank=True) # Field name made lowercase.
    creationtime = models.DateTimeField(db_column='CREATIONTIME') # Field name made lowercase.
    creationhost = models.CharField(max_length=384, db_column='CREATIONHOST', blank=True) # Field name made lowercase.
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME') # Field name made lowercase.
    modificationhost = models.CharField(max_length=384, db_column='MODIFICATIONHOST', blank=True) # Field name made lowercase.
    atlasrelease = models.CharField(max_length=192, db_column='ATLASRELEASE', blank=True) # Field name made lowercase.
    transformation = models.CharField(max_length=750, db_column='TRANSFORMATION', blank=True) # Field name made lowercase.
    homepackage = models.CharField(max_length=240, db_column='HOMEPACKAGE', blank=True) # Field name made lowercase.
    prodserieslabel = models.CharField(max_length=60, db_column='PRODSERIESLABEL', blank=True) # Field name made lowercase.
    prodsourcelabel = models.CharField(max_length=60, db_column='PRODSOURCELABEL', blank=True) # Field name made lowercase.
    produserid = models.CharField(max_length=750, db_column='PRODUSERID', blank=True) # Field name made lowercase.
    assignedpriority = models.IntegerField(db_column='ASSIGNEDPRIORITY') # Field name made lowercase.
    currentpriority = models.IntegerField(db_column='CURRENTPRIORITY') # Field name made lowercase.
    attemptnr = models.IntegerField(db_column='ATTEMPTNR') # Field name made lowercase.
    maxattempt = models.IntegerField(db_column='MAXATTEMPT') # Field name made lowercase.
    jobstatus = models.CharField(max_length=45, db_column='JOBSTATUS') # Field name made lowercase.
    jobname = models.CharField(max_length=768, db_column='JOBNAME', blank=True) # Field name made lowercase.
    maxcpucount = models.IntegerField(db_column='MAXCPUCOUNT') # Field name made lowercase.
    maxcpuunit = models.CharField(max_length=96, db_column='MAXCPUUNIT', blank=True) # Field name made lowercase.
    maxdiskcount = models.IntegerField(db_column='MAXDISKCOUNT') # Field name made lowercase.
    maxdiskunit = models.CharField(max_length=12, db_column='MAXDISKUNIT', blank=True) # Field name made lowercase.
    ipconnectivity = models.CharField(max_length=15, db_column='IPCONNECTIVITY', blank=True) # Field name made lowercase.
    minramcount = models.IntegerField(db_column='MINRAMCOUNT') # Field name made lowercase.
    minramunit = models.CharField(max_length=6, db_column='MINRAMUNIT', blank=True) # Field name made lowercase.
    starttime = models.DateTimeField(null=True, db_column='STARTTIME', blank=True) # Field name made lowercase.
    endtime = models.DateTimeField(null=True, db_column='ENDTIME', blank=True) # Field name made lowercase.
    cpuconsumptiontime = models.BigIntegerField(db_column='CPUCONSUMPTIONTIME') # Field name made lowercase.
    cpuconsumptionunit = models.CharField(max_length=384, db_column='CPUCONSUMPTIONUNIT', blank=True) # Field name made lowercase.
    commandtopilot = models.CharField(max_length=750, db_column='COMMANDTOPILOT', blank=True) # Field name made lowercase.
    transexitcode = models.CharField(max_length=384, db_column='TRANSEXITCODE', blank=True) # Field name made lowercase.
    piloterrorcode = models.IntegerField(db_column='PILOTERRORCODE') # Field name made lowercase.
    piloterrordiag = models.CharField(max_length=1500, db_column='PILOTERRORDIAG', blank=True) # Field name made lowercase.
    exeerrorcode = models.IntegerField(db_column='EXEERRORCODE') # Field name made lowercase.
    exeerrordiag = models.CharField(max_length=1500, db_column='EXEERRORDIAG', blank=True) # Field name made lowercase.
    superrorcode = models.IntegerField(db_column='SUPERRORCODE') # Field name made lowercase.
    superrordiag = models.CharField(max_length=750, db_column='SUPERRORDIAG', blank=True) # Field name made lowercase.
    ddmerrorcode = models.IntegerField(db_column='DDMERRORCODE') # Field name made lowercase.
    ddmerrordiag = models.CharField(max_length=1500, db_column='DDMERRORDIAG', blank=True) # Field name made lowercase.
    brokerageerrorcode = models.IntegerField(db_column='BROKERAGEERRORCODE') # Field name made lowercase.
    brokerageerrordiag = models.CharField(max_length=750, db_column='BROKERAGEERRORDIAG', blank=True) # Field name made lowercase.
    jobdispatchererrorcode = models.IntegerField(db_column='JOBDISPATCHERERRORCODE') # Field name made lowercase.
    jobdispatchererrordiag = models.CharField(max_length=750, db_column='JOBDISPATCHERERRORDIAG', blank=True) # Field name made lowercase.
    taskbuffererrorcode = models.IntegerField(db_column='TASKBUFFERERRORCODE') # Field name made lowercase.
    taskbuffererrordiag = models.CharField(max_length=900, db_column='TASKBUFFERERRORDIAG', blank=True) # Field name made lowercase.
    computingsite = models.CharField(max_length=384, db_column='COMPUTINGSITE', blank=True) # Field name made lowercase.
    computingelement = models.CharField(max_length=384, db_column='COMPUTINGELEMENT', blank=True) # Field name made lowercase.
    jobparameters = models.TextField(db_column='JOBPARAMETERS', blank=True) # Field name made lowercase.
    metadata = models.TextField(db_column='METADATA', blank=True) # Field name made lowercase.
    proddblock = models.CharField(max_length=765, db_column='PRODDBLOCK', blank=True) # Field name made lowercase.
    dispatchdblock = models.CharField(max_length=765, db_column='DISPATCHDBLOCK', blank=True) # Field name made lowercase.
    destinationdblock = models.CharField(max_length=765, db_column='DESTINATIONDBLOCK', blank=True) # Field name made lowercase.
    destinationse = models.CharField(max_length=750, db_column='DESTINATIONSE', blank=True) # Field name made lowercase.
    nevents = models.IntegerField(db_column='NEVENTS') # Field name made lowercase.
    grid = models.CharField(max_length=150, db_column='GRID', blank=True) # Field name made lowercase.
    cloud = models.CharField(max_length=150, db_column='CLOUD', blank=True) # Field name made lowercase.
    cpuconversion = models.DecimalField(decimal_places=4, null=True, max_digits=11, db_column='CPUCONVERSION', blank=True) # Field name made lowercase.
    sourcesite = models.CharField(max_length=108, db_column='SOURCESITE', blank=True) # Field name made lowercase.
    destinationsite = models.CharField(max_length=108, db_column='DESTINATIONSITE', blank=True) # Field name made lowercase.
    transfertype = models.CharField(max_length=30, db_column='TRANSFERTYPE', blank=True) # Field name made lowercase.
    taskid = models.IntegerField(null=True, db_column='TASKID', blank=True) # Field name made lowercase.
    cmtconfig = models.CharField(max_length=750, db_column='CMTCONFIG', blank=True) # Field name made lowercase.
    statechangetime = models.DateTimeField(null=True, db_column='STATECHANGETIME', blank=True) # Field name made lowercase.
    proddbupdatetime = models.DateTimeField(null=True, db_column='PRODDBUPDATETIME', blank=True) # Field name made lowercase.
    lockedby = models.CharField(max_length=384, db_column='LOCKEDBY', blank=True) # Field name made lowercase.
    relocationflag = models.IntegerField(null=True, db_column='RELOCATIONFLAG', blank=True) # Field name made lowercase.
    jobexecutionid = models.BigIntegerField(null=True, db_column='JOBEXECUTIONID', blank=True) # Field name made lowercase.
    vo = models.CharField(max_length=48, db_column='VO', blank=True) # Field name made lowercase.
    pilottiming = models.CharField(max_length=300, db_column='PILOTTIMING', blank=True) # Field name made lowercase.
    workinggroup = models.CharField(max_length=60, db_column='WORKINGGROUP', blank=True) # Field name made lowercase.
    processingtype = models.CharField(max_length=192, db_column='PROCESSINGTYPE', blank=True) # Field name made lowercase.
    produsername = models.CharField(max_length=180, db_column='PRODUSERNAME', blank=True) # Field name made lowercase.
    ninputfiles = models.IntegerField(null=True, db_column='NINPUTFILES', blank=True) # Field name made lowercase.
    countrygroup = models.CharField(max_length=60, db_column='COUNTRYGROUP', blank=True) # Field name made lowercase.
    batchid = models.CharField(max_length=240, db_column='BATCHID', blank=True) # Field name made lowercase.
    parentid = models.BigIntegerField(null=True, db_column='PARENTID', blank=True) # Field name made lowercase.
    specialhandling = models.CharField(max_length=240, db_column='SPECIALHANDLING', blank=True) # Field name made lowercase.
    jobsetid = models.BigIntegerField(null=True, db_column='JOBSETID', blank=True) # Field name made lowercase.
    corecount = models.IntegerField(null=True, db_column='CORECOUNT', blank=True) # Field name made lowercase.
    ninputdatafiles = models.IntegerField(null=True, db_column='NINPUTDATAFILES', blank=True) # Field name made lowercase.
    inputfiletype = models.CharField(max_length=96, db_column='INPUTFILETYPE', blank=True) # Field name made lowercase.
    inputfileproject = models.CharField(max_length=192, db_column='INPUTFILEPROJECT', blank=True) # Field name made lowercase.
    inputfilebytes = models.BigIntegerField(null=True, db_column='INPUTFILEBYTES', blank=True) # Field name made lowercase.
    noutputdatafiles = models.IntegerField(null=True, db_column='NOUTPUTDATAFILES', blank=True) # Field name made lowercase.
    outputfilebytes = models.BigIntegerField(null=True, db_column='OUTPUTFILEBYTES', blank=True) # Field name made lowercase.
    jobmetrics = models.CharField(max_length=1500, db_column='JOBMETRICS', blank=True) # Field name made lowercase.
    workqueue_id = models.IntegerField(null=True, db_column='WORKQUEUE_ID', blank=True) # Field name made lowercase.
    jeditaskid = models.BigIntegerField(null=True, db_column='JEDITASKID', blank=True) # Field name made lowercase.
    jobstatus = models.CharField(null=True, max_length=80, db_column='JOBSTATUS', blank=True)
    actualcorecount = models.IntegerField(null=True, db_column='ACTUALCORECOUNT', blank=True)
    reqid = models.BigIntegerField(null=True, db_column='REQID', blank=True) # Field name made lowercase.
    # nucleus = models.CharField(max_length=200, db_column='nucleus', blank=True) # Field name made lowercase.
    # jobsubstatus = models.CharField(null=True, max_length=80, db_column='JOBSUBSTATUS', blank=True)
    # eventservice = models.IntegerField(null=True, db_column='EVENTSERVICE', blank=True) # Field name made lowercase.
    #
    # maxrss = models.BigIntegerField(null=True, db_column='maxrss', blank=True) # Field name made lowercase.
    # maxvmem = models.BigIntegerField(null=True, db_column='maxvmem', blank=True) # Field name made lowercase.
    # maxswap = models.BigIntegerField(null=True, db_column='maxswap', blank=True) # Field name made lowercase.
    # maxpss = models.BigIntegerField(null=True, db_column='maxpss', blank=True) # Field name made lowercase.
    # avgrss = models.BigIntegerField(null=True, db_column='avgrss', blank=True) # Field name made lowercase.
    # avgvmem = models.BigIntegerField(null=True, db_column='avgvmem', blank=True) # Field name made lowercase.
    # avgswap = models.BigIntegerField(null=True, db_column='avgswap', blank=True) # Field name made lowercase.
    # avgpss = models.BigIntegerField(null=True, db_column='avgpss', blank=True) # Field name made lowercase.
    # maxwalltime = models.BigIntegerField(null=True, db_column='maxwalltime', blank=True) # Field name made lowercase.



    def __str__(self):
        return 'PanDA:' + str(self.pandaid)

#     # __setattr__
#     def __setattr__(self, name, value):
#         super(PandaJob, self).__setattr__(name, value)
# 
#     # __getattr__
#     def __getattr__(self, name):
#         return super(PandaJob, self).__getattr__(name)

    # __getitem__
    def __getitem__(self, name):
#        return super(HTCondorJob, self).__getattr__(name)
        return self.__dict__[name]

    class Meta:
        abstract = True

class Jobsactive4(PandaJob):
    class Meta:
#        managed = False
        db_table = u'jobsactive4'

class Jobsarchived(PandaJob):
    class Meta:
#        managed = False
        db_table = u'jobsarchived'

class Jobsarchived4(PandaJob):
    class Meta:
#        managed = False
        db_table = u'jobsarchived4'

class Jobsdefined4(PandaJob):
    class Meta:
#        managed = False
        db_table = u'jobsdefined4'

    # __getitem__
    def __getitem__(self, name):
#        return super(HTCondorJob, self).__getattr__(name)
        return self.__dict__[name]

class Jobswaiting4(PandaJob):
    class Meta:
#        managed = False
        db_table = u'jobswaiting4'

class Filestable4(models.Model):
    row_id = models.BigIntegerField(db_column='ROW_ID', primary_key=True)
    pandaid = models.BigIntegerField(db_column='PANDAID')
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME', primary_key=True)
    guid = models.CharField(max_length=192, db_column='GUID', blank=True)
    lfn = models.CharField(max_length=768, db_column='LFN', blank=True)
    type = models.CharField(max_length=60, db_column='TYPE', blank=True)
    dataset = models.CharField(max_length=765, db_column='DATASET', blank=True)
    status = models.CharField(max_length=192, db_column='STATUS', blank=True)
    proddblock = models.CharField(max_length=765, db_column='PRODDBLOCK', blank=True)
    proddblocktoken = models.CharField(max_length=750, db_column='PRODDBLOCKTOKEN', blank=True)
    dispatchdblock = models.CharField(max_length=765, db_column='DISPATCHDBLOCK', blank=True)
    dispatchdblocktoken = models.CharField(max_length=750, db_column='DISPATCHDBLOCKTOKEN', blank=True)
    destinationdblock = models.CharField(max_length=765, db_column='DESTINATIONDBLOCK', blank=True)
    destinationdblocktoken = models.CharField(max_length=750, db_column='DESTINATIONDBLOCKTOKEN', blank=True)
    destinationse = models.CharField(max_length=750, db_column='DESTINATIONSE', blank=True)
    fsize = models.BigIntegerField(db_column='FSIZE')
    md5sum = models.CharField(max_length=108, db_column='MD5SUM', blank=True)
    checksum = models.CharField(max_length=108, db_column='CHECKSUM', blank=True)
    scope = models.CharField(max_length=90, db_column='SCOPE', blank=True)
    jeditaskid = models.BigIntegerField(null=True, db_column='JEDITASKID', blank=True)
    datasetid = models.BigIntegerField(null=True, db_column='DATASETID', blank=True)
    fileid = models.BigIntegerField(null=True, db_column='FILEID', blank=True)
    attemptnr = models.IntegerField(null=True, db_column='ATTEMPTNR', blank=True)
    class Meta:
        db_table = u'filestable4'
        unique_together = ('row_id', 'modificationtime')

class MetaTable(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID', primary_key=True)
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME', primary_key=True)
    metadata = models.TextField(db_column='METADATA', blank=True)
    class Meta:
        managed = False
        db_table = u'metatable'

class JobParamsTable(models.Model):
    pandaid = models.BigIntegerField(db_column='PANDAID', primary_key=True)
    modificationtime = models.DateTimeField(db_column='MODIFICATIONTIME', primary_key=True)
    jobparameters = models.TextField(db_column='JOBPARAMETERS', blank=True)
    class Meta:
        managed = False
        db_table = u'jobparamstable'
