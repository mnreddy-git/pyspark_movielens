import logging
import os
from datetime import datetime

# Gets or creates a logger
logger = logging.getLogger(__name__)

# set log level
logger.setLevel(logging.DEBUG)

#C:\Users\naras_000\PycharmProjects\MNR_SPARKE2EProjects\logs

# define file handler and set formatter
project_path = os.path.dirname(__file__)
print("project_path is : ",project_path)
parentpath=os.path.abspath(os.path.join(project_path,'..'))
print("parentpath is : ",parentpath)
pparentpath=os.path.abspath(os.path.join(parentpath,'..'))
print("pparentpath is : ",pparentpath)
logFileName=datetime.now().strftime('MovieLens_%d%m%Y_%H_%M.log')
logFilePath= os.path.abspath(os.path.join(pparentpath,'log\\MovieAnalytics', logFileName))
print("logFilePath is : ",logFilePath)

file_handler = logging.FileHandler(logFilePath)
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)

# add file handler to logger
#logger.addHandler(file_handler)

if __name__=="__main__":
    logger.debug('A debug message')
    logger.info('An info message')
    logger.warning('Something is not right.')
    logger.error('A Major error has happened.')
    logger.critical('Fatal error. Cannot continue')