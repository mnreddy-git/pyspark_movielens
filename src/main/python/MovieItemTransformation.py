import pyspark
from pyspark.sql import functions as F
import MovieLensAnalysis.service.CsvReader as CsvReader
from pyspark.sql.window import Window
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

from MovieLensAnalysis.util.MovieLensConstants import MvlUtils

class MovieTransformation:
    '''THIS CLASS HOLDS ALL THE PRE AND POST TRANSFORMATION REQs'''
    mvlUtils=MvlUtils()
    def __str__(self):
        return "MOVIE TRANSFORMATION OBJECT"

    def __init__(self,spark,sc):
        self.spark=spark
        self.sc=sc

    def createStructSchema(self,schemaString, dataType, delimiter):
        logger.debug("<ML -createStructSchema><schemaString is {schemaString} >".format(schemaString=schemaString))
        logger.debug("<ML -createStructSchema><delimiter is {delimiter}>".format(delimiter=delimiter))
        structFieldList = [self.mvlUtils.STRUCT_FIELD(colName, dataType, True) for colName in schemaString.split(delimiter)]
        #logger.debug("<ML -createStructSchema><type(structFieldList) is > ",type(structFieldList))
        logger.debug("<ML -createStructSchema><structFieldList is > {structList}".format(structList=structFieldList))
        return structFieldList

    def combineTheSchema(self,structListA, structListB):
        return structListA + structListB

    def loadMovieData(self):
        itemSchema = self.generateItemSchema()
        itemDF = self.loadItemDataAsDF(itemSchema)
        userSchema = self.generateUserSchema()
        userDF = self.loadUserAsDF(userSchema)
        userDataSchema=self.generateUserDataSchema()
        userDataDF=self.loadUserDataAsDF(userDataSchema)
        return itemDF, userDF,userDataDF

    def generateItemSchema(self):
        #print("<ML-generateItemSchema>")
        nonGenreStructSchema = self.createStructSchema(self.mvlUtils.nonGenreString,
                                                                self.mvlUtils.STRING_TYPE,
                                                                self.mvlUtils.PIPE_DELIMITER)
        logger.debug("<ML-generateItemSchema><nonGenreStructSchema> is {nonGenreStructSchema}".format(nonGenreStructSchema=nonGenreStructSchema))

        genreStructSchema = self.createStructSchema(self.mvlUtils.genreString,
                                                        self.mvlUtils.INTEGER_TYPE,
                                                        self.mvlUtils.PIPE_DELIMITER)
        logger.debug("<ML-generateItemSchema><genreStructSchema> is {genreStructSchema}".format(genreStructSchema=genreStructSchema))

        itemSchemaCombined = self.combineTheSchema(nonGenreStructSchema,
                                                            genreStructSchema)
        itemSchema = self.mvlUtils.STRUCT_TYPE(itemSchemaCombined)
        logger.debug("<ML-generateItemSchema><itemSchema is > - {itemSchema}".format(itemSchema=itemSchema))
        return itemSchema

    def generateUserSchema(self):
        userSchemaIntegersStructField = self.createStructSchema(self.mvlUtils.userSchemaIntegers,
                                                                         self.mvlUtils.INTEGER_TYPE,
                                                                         self.mvlUtils.PIPE_DELIMITER)
        userSchemaStringStructField = self.createStructSchema(self.mvlUtils.userSchemaStrings,
                                                                       self.mvlUtils.STRING_TYPE,
                                                                       self.mvlUtils.PIPE_DELIMITER)
        userSchemaCombined = self.combineTheSchema(userSchemaIntegersStructField,
                                                            userSchemaStringStructField)
        userSchema = self.mvlUtils.STRUCT_TYPE(userSchemaCombined)
        logger.debug("<ML-generateUserSchema><userSchema is {userSchema}> ".format(userSchema=userSchema))
        return userSchema

    def generateUserDataSchema(self):
        userDataSchemaIntStructField=self.createStructSchema(self.mvlUtils.userDataColumnsIntegers,
                                                                      self.mvlUtils.INTEGER_TYPE,
                                                                      self.mvlUtils.PIPE_DELIMITER)
        userDataSchemaStructType=self.mvlUtils.STRUCT_TYPE(userDataSchemaIntStructField)
        logger.debug("<ML-generateUserDataSchema><userDataSchemaStructType> is {userDataSchemaStructType}".format(userDataSchemaStructType=userDataSchemaStructType))
        return userDataSchemaStructType

    def generateGenreCountSchema(self):
        genreCountStructField=self.createStructSchema(self.mvlUtils.genreCountSchemaStr,
                                self.mvlUtils.STRING_TYPE,
                                self.mvlUtils.PIPE_DELIMITER)
        return self.mvlUtils.STRUCT_TYPE(genreCountStructField)

    def loadItemDataAsDF(self,itemSchema):
        itemDF = CsvReader.readCSVasDF(self.spark,
                                       self.mvlUtils.HEADER_FALSE,
                                       self.mvlUtils.INFERSCHEMA_FALSE,
                                       itemSchema,
                                       self.mvlUtils.PIPE_DELIMITER,
                                       self.mvlUtils.BASE_LOCATION + "u.item")
        return itemDF

    def loadUserAsDF(self,userSchema):
        userDF = CsvReader.readCSVasDF(self.spark,
                                       self.mvlUtils.HEADER_FALSE,
                                       self.mvlUtils.INFERSCHEMA_FALSE,
                                       userSchema,
                                       self.mvlUtils.PIPE_DELIMITER,
                                       self.mvlUtils.BASE_LOCATION + "u.user")
        return userDF

    def loadUserDataAsDF(self,userDataSchema):
        userDataDF=CsvReader.readCSVasDF(self.spark,
                                         self.mvlUtils.HEADER_FALSE,
                                         self.mvlUtils.INFERSCHEMA_FALSE,
                                         userDataSchema,
                                         self.mvlUtils.PIPE_DELIMITER,
                                         self.mvlUtils.BASE_LOCATION+"u.data")
        return userDataDF

    def findGenderAggCount(self,userDF):
        genderAggCntDF = userDF.groupBy("gender").\
            agg(F.count("gender").alias("CountOfEmployeesByGender")).\
            sort(F.desc("CountOfEmployeesByGender"))
        return genderAggCntDF

    def findOccupAggCount(self,userDF):
        occupAggCntDF = userDF.groupBy("occupation"). \
            agg(F.count("occupation").alias("CountOfEmployeesByOccup")). \
            sort(F.desc("CountOfEmployeesByOccup"))
        return occupAggCntDF

    def findTopOccupGivenRating(self,userDF,userDataDF,itemDF):
        topOccupGivenRatingDF=userDF.\
            join(userDataDF,userDF.userid==userDataDF.user_id,"inner").\
            join(itemDF,itemDF["movieid"]==userDataDF["item_id"],"inner").filter("timestamp is not null").\
            groupBy(userDF.occupation).\
            agg(F.sum(userDataDF["rating"]).alias("sum_rating"),
                F.countDistinct(userDataDF.user_id).alias("count_distinct_users"),
                F.countDistinct(userDataDF.item_id).alias("count_distinct_movies")).\
            orderBy(F.desc("sum_rating"))
        return topOccupGivenRatingDF

    def findTopMoviesPerYr(self,userDataDf,itemDF):
        window_yr_item_noSort=Window.\
            partitionBy("yr","item_id")
        window_yr_sort_sumDesc=Window.\
            partitionBy("yr").\
            orderBy(F.desc("sumRatingPerMvePerYr"))
        rankedUserDataDF=userDataDf.filter("timestamp is not null").\
            select("item_id","rating","timestamp",\
                   F.from_unixtime(userDataDf.timestamp,"YYYY-MM-dd").alias("yr_mm_dd"),\
                   F.from_unixtime(userDataDf.timestamp,"YYYY").alias("yr")).\
            select("*",F.sum("rating").over(window_yr_item_noSort).alias("sumRatingPerMvePerYr")).\
            select("*",F.dense_rank().over(window_yr_sort_sumDesc).alias("rnk"))
        rankedUserDataWithMveNameDF=rankedUserDataDF.join(itemDF,rankedUserDataDF.item_id==itemDF.movieid,"inner").\
            select("item_id","yr","sumRatingPerMvePerYr","movietitle","rnk")
        top10MvsPerYrDF=rankedUserDataWithMveNameDF.\
            filter(rankedUserDataWithMveNameDF.rnk<=10).\
            select("item_id","movietitle","yr","sumRatingPerMvePerYr").\
            distinct()
        top10MvsPerYrDF.show()
        return top10MvsPerYrDF

    def findTopMovies(self,userDataDF,itemDF):
        windowPerMovie=Window.partitionBy("item_id")
        windowPerMovieSortRating=Window().orderBy(F.desc("sumOfRating"))
        topMoviesDF=userDataDF.filter("timestamp is not null").join(itemDF,userDataDF.item_id==itemDF.movieid,"inner").\
            select("item_id","movietitle",F.sum("rating").over(windowPerMovie).alias("sumOfRating")).\
            select("*").distinct().select("*",F.row_number().over(windowPerMovieSortRating).alias("rnk"))
        return topMoviesDF

    def saveResultToCSVFile(self,resultDF,moduleName):
        writeStatus=CsvReader.writeToCsvFile(resultDF,
                                 self.mvlUtils.NUMPARTITIONSTOWRITE,
                                 self.mvlUtils.HEADER_TRUE,
                                 self.mvlUtils.NO_COMPRESSION,
                                 self.mvlUtils.COMMA_DELIMITER,
                                 self.mvlUtils.OVERWRITE_MODE,
                                 moduleName,
                                 self.mvlUtils.TARGET_LOCATION)
        if(writeStatus):
            #print("<ML> - WRITE STATUS OF {} module is SUCCESS".format(moduleName))
            logger.debug("<ML> - WRITE STATUS OF {} module is SUCCESS".format(moduleName))
        else:
            #print("<ML> - WRITE STATUS OF {} module is FAILURE".format(moduleName))
            logger.debug("<ML> - WRITE STATUS OF {} module is FAILURE".format(moduleName))
        return None

    def countOfMvsGenreWise(self,itemDF):
        emptyRdd = self.sc.emptyRDD()
        genreNamesString=self.mvlUtils.genreString
        genreCountTempDF = self.spark.createDataFrame(emptyRdd, self.generateGenreCountSchema())
        for genre in genreNamesString.split("|"):
            condition = genre + "==1"  ## 'Action==1'
            logger.debug("condition is {cond}".format(cond=condition))
            tmpDF = itemDF.filter(condition).\
                select(F.lit(genre).alias("Genre"),
                       F.countDistinct("movieid").alias("countOfMovies").cast(self.mvlUtils.INTEGER_TYPE))
            genreCountTempDF = genreCountTempDF.union(tmpDF)
        printschema=genreCountTempDF.printSchema()
        logger.debug("Schema is {schema}".format(schema=printschema))
        genreCountTempDF.show()
        mveCountGenreWiseSortedDF=genreCountTempDF.orderBy(F.desc("countOfMovies"))
        return mveCountGenreWiseSortedDF

if __name__=="__main__":
    pass

#find the occupation from  which the users has given more ratings.