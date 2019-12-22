import pyspark
from pyspark.sql import types as T
import yaml
import os
import sys
from MovieLensAnalysis.util.customLogger  import logFilePath
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


project_path = os.path.dirname(__file__)
#print("project_path is : ",project_path)
conf_path = os.path.abspath(os.path.join(project_path, '..', 'conf', 'MovieLens.yaml'))
#print("conf_path is : ",conf_path)


def findSQLType(typeString):
    switcher = {
        "STRING_TYPE": T.StringType(),
        "INTEGER_TYPE": T.IntegerType(),
        "STRUCT_TYPE": T.StructType,
        "STRUCT_FIELD": T.StructField
    }
    return switcher.get(typeString, "nothing")

class MvlUtils():
    with open(conf_path, 'r') as conf_stream:
        try:
            conf = yaml.load(conf_stream)
            pipe_delim=conf['movieLens']['delimiters']['PIPE_DELIMITER']

            PIPE_DELIMITER = conf['movieLens']['delimiters']['PIPE_DELIMITER']
            TAB_DELIMITER = conf['movieLens']['delimiters']['TAB_DELIMITER']
            COMMA_DELIMITER = conf['movieLens']['delimiters']['COMMA_DELIMITER']
            DOLLAR_DELIMITER = conf['movieLens']['delimiters']['DOLLAR_DELIMITER']

            STRING_TYPE = findSQLType(conf['movieLens']['types']['STRING_TYPE'])
            INTEGER_TYPE = findSQLType(conf['movieLens']['types']['INTEGER_TYPE'])
            STRUCT_TYPE = findSQLType(conf['movieLens']['types']['STRUCT_TYPE'])
            STRUCT_FIELD = findSQLType(conf['movieLens']['types']['STRUCT_FIELD'])

            HEADER_FALSE = conf['movieLens']['options']['HEADER_FALSE']
            HEADER_TRUE = conf['movieLens']['options']['HEADER_TRUE']
            INFERSCHEMA_TRUE = conf['movieLens']['options']['INFERSCHEMA_TRUE']
            INFERSCHEMA_FALSE = conf['movieLens']['options']['INFERSCHEMA_FALSE']

            nonGenreString = conf['movieLens']['schemaString']['nonGenreString']
            genreString = conf['movieLens']['schemaString']['genreString']
            userSchemaIntegers = conf['movieLens']['schemaString']['userSchemaIntegers']
            userSchemaStrings = conf['movieLens']['schemaString']['userSchemaStrings']
            userDataColumnsIntegers = conf['movieLens']['schemaString']['userDataColumnsIntegers']
            genreCountSchemaStr = conf['movieLens']['schemaString']['genreCountSchemaStr']

            BASE_LOCATION = conf['movieLens']['paths']['BASE_LOCATION']
            TARGET_LOCATION = conf['movieLens']['paths']['TARGET_LOCATION']

            NUMPARTITIONSTOWRITE = conf['movieLens']['options']['NUMPARTITIONSTOWRITE']
            NO_COMPRESSION = conf['movieLens']['options']['NO_COMPRESSION']
            GZIP_COMPRESSION = conf['movieLens']['options']['GZIP_COMPRESSION']
            SNAPPY_COMPRESSION = conf['movieLens']['options']['SNAPPY_COMPRESSION']
            OVERWRITE_MODE = conf['movieLens']['options']['OVERWRITE_MODE']
            IGNORE_MODE = conf['movieLens']['options']['IGNORE_MODE']
            APPEND_MODE = conf['movieLens']['options']['APPEND_MODE']


        except Exception as e:
            print(str(e))
            print(sys.exc_info())
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.debug(
                "<ML-EXCEPTION!> {type} ,{name} ,{line}".format(type=exc_type, name=fname, line=exc_tb.tb_lineno))
            exit(1)


if __name__=='__main__':
    MvlUtils()
