# pyspark_movielens
End2End project demonstration in Pyspark .

PreRequisite:
1.Refer to dataset folder for the set of input files..
2.Update the downloaded folder path details in conf/MovieLens.yaml file - BASE_LOCATION and TARGET_LOCATION
3.Download the repository and try to execute witn Pycharm

=======================Problem Statement==================
1.findGenderAggCount
find the count of users by genderwise who participated in rating

2.findOccupAggCount
find the count of users by occuoation wise who participated in rating

3.findTopOccupGivenRating
find the Ocupation from which more users have given rating

4.findTopMoviesPerYr
find the top movies by rating per each year

5.findTopMovies
find the overall top movies

6.countOfMvsGenreWise

Save the results to a folder with overwrite mode and csv format.

===============================
TO support Spark-Submit:
Since it involved different modules, we need to create .egg file from the project . 

To create .egg file:
1. Create setup.py - https://www.jetbrains.com/help/pycharm/creating-and-running-setup-py.html
2. Create.egg file - https://stackoverflow.com/questions/2026395/how-to-create-python-egg-file

sample setup.py is added to repo. 

Spark-Submit command:
spark-submit --master local[*] --deploy-mode client --py-files  C:\\Users\\naras_000\\PycharmProjects\\MNR_SPARKE2EProjects\\dist\\MNR_PYSPARK_PROJ_MovieLens-1.0-py2.7.egg  C:\\Users\\naras_000\\PycharmProjects\\MNR_SPARKE2EProjects\\MovieLensAnalysis\\src\\main\\python\\Movie_ItemAnalysis.py
