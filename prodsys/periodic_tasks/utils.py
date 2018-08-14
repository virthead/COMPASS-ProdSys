import sys, os
import subprocess
from django.conf import settings
import logging
from django.core.wsgi import get_wsgi_application

sys.path.append(os.path.join(os.path.dirname(__file__), '../../')) # fix me in case of using outside the project
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compass.settings")
application = get_wsgi_application()

def check_process(process, pid):
    returnprocess = False
    s = subprocess.Popen(["ps", "ax"], stdout=subprocess.PIPE)
    for x in s.stdout:
        if x.find('/bin/sh') != -1:
            continue
        if x.find(process) != -1 and x.find(pid) == -1:
            returnprocess = True

    return returnprocess

def getRotatingFileHandler(logger, log_name):
    logger.propagate = False
    
    for handler in logger.handlers:
        logger.handlers.pop()
    
    FORMAT = '[%(asctime)s] %(levelname)s [%(funcName)s:%(lineno)d] %(message)s'
    fh = logging.handlers.TimedRotatingFileHandler(os.path.join(settings.LOGGING_DIR, log_name), when='midnight', backupCount=7)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(FORMAT))
    
    logger.addHandler(fh)
    
    return
