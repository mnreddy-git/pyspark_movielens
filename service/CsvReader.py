from pyspark import SparkConf
from pyspark.sql import SparkSession
import os,sys
from MovieLensAnalysis.util.customLogger import logFilePath
import logging

# Gets or creates a logger
logger = logging.getLogger(__name__)

# set log level
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(logFilePath)
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)


def readCSVasDF(sparkSession,isHeader,isInferSchema,inputSchema,inputDelimiter,inputFilePath):
    csvDF=sparkSession.read.format("csv").\
        option("header",isHeader).\
        option("inferSchema",isInferSchema).\
        option("sep",inputDelimiter).\
        schema(inputSchema).\
        load(inputFilePath)
    return csvDF
def writeToCsvFile(resultDF,numPartitions,isHeader,compressionFormat,delimiter,writeMode,moduleName,destinationPath):
    try:
        path=destinationPath.format(moduleName+"//")
        logger.debug("Writing to path {path}".format(path=path))
        resultDF.repartition(numPartitions).write.format("csv").\
            option("header",isHeader).\
            option("compress",compressionFormat).\
            option("sep",delimiter). \
            mode(writeMode).\
            save(path)
        return True
    except Exception as e:
        logger.debug("Exception in writing Dataframe - {msg}".format(msg=str(e)))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.debug("<ML-EXCEPTION!> {type},{name},{lineno}".format(type=exc_type, name=fname, lineno=exc_tb.tb_lineno))
        exit(1)
        return False


if __name__=='__main__':
    pass