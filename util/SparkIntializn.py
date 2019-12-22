from pyspark import SparkConf
from pyspark.sql import SparkSession
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


def sparkInitlzn():
    sconf=SparkConf().setMaster("local[*]").\
        setAppName("MNR-MovieLensAnalysis")
    spark = SparkSession.builder.config(conf=sconf).getOrCreate()
    sc=spark.sparkContext
    logger.debug("<ML - sparkInitlzn>  Spark Session is created , version is {ver}".format(ver=sc.version ))
    sc.setLogLevel("WARN")
    return sc,spark
def resourceCleanup(sc):
    logger.debug("<ML - sparkInitlzn>  Called Resource cleanUp")
    sc.stop()
    logger.debug(" <ML - sparkInitlzn>  Resources are cleaned")

if __name__=='__main__':
    print("<ML - sparkInitlzn>  In Main Method")
    scontext,ssession=sparkInitlzn()
    print("<ML - sparkInitlzn>  From Main Method  - " , scontext.version)
    resourceCleanup(scontext)





