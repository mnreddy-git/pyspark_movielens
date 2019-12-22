import logging
import os
from datetime import datetime


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
logFileName=datetime.now().strftime('MovieLens_%d%m%Y_%H_%M.log')
logFilePath= os.path.abspath(os.path.join(baseFolder,'logs\\MovieAnalytics', logFileName))
print("logFilePath is : ",logFilePath)

if __name__=="__main__":
    pass
    # logger.debug('A debug message')
    #logger.info('An info message')
    #logger.warning('Something is not right.')
   # logger.error('A Major error has happened.')
    #logger.critical('Fatal error. Cannot continue')