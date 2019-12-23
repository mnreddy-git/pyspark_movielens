import logging
import os
from datetime import datetime
import yaml
import os
import sys



#project_path = os.path.dirname(__file__)
#print("project_path is : ",project_path)
#logFileName=datetime.now().strftime('MovieLens_%d%m%Y_%H_%M.log')
#logFilePath= os.path.abspath(os.path.join(project_path, '..', 'log', logFileName))
#print("conf_path is : ",conf_path)

project_folder_path = os.path.dirname(__file__)
#print("project_path is : ",project_path)
project_path=os.path.abspath(os.path.join(project_folder_path,'..'))
#print("parentpath is : ",parentpath)
baseFolder=os.path.abspath(os.path.join(project_path,'..'))
#print("pparentpath is : ",pparentpath)

conf_path = os.path.abspath(os.path.join(project_folder_path, '..', 'conf', 'MovieLens.yaml'))

def getLogPath():
    with open(conf_path, 'r') as conf_stream:
        conf = yaml.load(conf_stream)
        logpath=conf['movieLens']['paths']['LOG_LOCATION']
        return logpath

logFileName=datetime.now().strftime('MovieLens_%d%m%Y_%H_%M.log')
logFilePath= os.path.abspath(os.path.join(getLogPath(),'MovieAnalytics', logFileName))
print("logFilePath is : ",logFilePath)

if __name__=="__main__":
    pass
    # logger.debug('A debug message')
    #logger.info('An info message')
    #logger.warning('Something is not right.')
   # logger.error('A Major error has happened.')
    #logger.critical('Fatal error. Cannot continue')