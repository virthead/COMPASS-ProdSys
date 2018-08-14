#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import saga
import os
import logging
import time 
from utils import check_process, getRotatingFileHandler

logger = logging.getLogger('periodic_tasks_logger')
getRotatingFileHandler(logger, 'periodic_tasks.get_proxy.log')
logger.info('Starting %s' % __file__)

def main():
    proxy_local = '/tmp/x509up_u%s' % os.geteuid()
    try:
        ctx = saga.Context("UserPass")
        ctx.user_id = "*****" # remote login name
        ctx.user_pass = "*****" # password
        if os.path.isfile(proxy_local):
            old_proxy = os.stat(proxy_local).st_mtime
            logger.info("Current proxy: %s" % time.ctime(old_proxy)) 

        logger.info('connect to pandawms')
        session = saga.Session()
        session.add_context(ctx)
    
        js = saga.job.Service("ssh://pandawms.jinr.ru", session=session)

        jd = saga.job.Description()        
        jd.executable      = "voms-proxy-init -voms vo.compass.cern.ch:/vo.compass.cern.ch/Role=production --valid 96:00 -q -old --out /home/virthead/x509up_u500 -pwstdin < proxy/gp"
        jd.output          = "/home/virthead/proxy/GetProxy.stdout"  # full path to remote stdout
        jd.error           = "/home/virthead/proxy/GetProxy.stderr"  # full path to remote srderr

        myjob = js.create_job(jd)
        myjob.run()
        myjob.wait()
        old_proxy = 0.0
        outfilesource = 'sftp://pandawms.jinr.ru/home/virthead/x509up_u500'   # path to proxy
        outfiletarget = 'file://localhost%s' % proxy_local
        logger.info('start loading proxy')
        load = True
        while load:
            out = saga.filesystem.File(outfilesource, session=session)
            out.copy(outfiletarget)
            new_proxy = os.stat(proxy_local).st_mtime
            if new_proxy > old_proxy:
                load = False
        logger.info('proxy loaded')
        new_proxy = os.stat(proxy_local).st_mtime
        logger.info("New proxy: %s" % time.ctime(new_proxy))
        return 0
        
    except saga.SagaException, ex:
        # Catch all saga exceptions
        logger.exception("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        logger.exception(" \n*** Backtrace:\n %s" % ex.traceback)
        return -1
    
if __name__ == "__main__":
    sys.exit(main())
