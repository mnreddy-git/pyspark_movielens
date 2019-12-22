import MovieLensAnalysis.util.SparkIntializn as spy
from MovieItemTransformation import *
import sys,os
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

def performMovieAnalytics():
    try:
        sc, spark = spy.sparkInitlzn()
        logger.debug("<ML>  Spark Initialization is Done ")
        logger.debug("<ML>  Spark Initialization is Done ")
        logger.debug("<ML>  Spark Version is {version}".format(version= sc.version))

        mvTransformation = MovieTransformation(spark, sc)  ## Create MovieItemTransformation Object
        itemDF, userDF, userDataDF = mvTransformation.loadMovieData()
        itemDF.show()
        itemDF.printSchema()
        userDF.show()
        userDF.printSchema()
        userDataDF.show()
        userDataDF.printSchema()
        ##Triggering Transformations
        ## Find the Count of users per Gender
        genderAggCntDF = mvTransformation.findGenderAggCount(userDF)
        ## Find the Count of users per Occupation
        occupAggCntDF = mvTransformation.findOccupAggCount(userDF)
        ## Find the Top occupation from which most users provided Rating
        findTopOccupFromRating = mvTransformation.findTopOccupGivenRating(userDF, userDataDF, itemDF)
        ## Find the Top Movies per yearwise
        findTopMoviesPeryrByRating = mvTransformation.findTopMoviesPerYr(userDataDF, itemDF)
        ## Find the Overall Top Movies
        findTopMoviesOveraAll = mvTransformation.findTopMovies(userDataDF, itemDF)
        ## Find the Count of Movies Genrewise
        findCountOfMveGenreWise = mvTransformation.countOfMvsGenreWise(itemDF)
        ## DISPLAY THE RESULTS

        genderAggCntDF.show()
        occupAggCntDF.show()
        findTopOccupFromRating.show(5)
        findTopMoviesPeryrByRating.show()
        findTopMoviesOveraAll.show()
        findCountOfMveGenreWise.show()

        ## SAVE THE DATAFRAMES TO FILES
        mvTransformation.saveResultToCSVFile(genderAggCntDF, "genderAggCntDF")
        mvTransformation.saveResultToCSVFile(occupAggCntDF, "occupAggCntDF")
        mvTransformation.saveResultToCSVFile(findTopOccupFromRating, "findTopOccupFromRating")
        mvTransformation.saveResultToCSVFile(findTopMoviesPeryrByRating, "findTopMoviesPeryrByRating")
        mvTransformation.saveResultToCSVFile(findTopMoviesOveraAll, "findTopMoviesOveraAll")
        mvTransformation.saveResultToCSVFile(findCountOfMveGenreWise, "findCountOfMveGenreWise")

    except Exception as e:
        print(str(e))
        print(sys.exc_info())
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.debug("<ML-EXCPETION!> {type} ,{name} ,{line}".format(type=exc_type, name=fname, line=exc_tb.tb_lineno))
        exit(1)
    finally:
        spy.resourceCleanup(sc)


if __name__=="__main__":
    performMovieAnalytics()

