# Databricks notebook source
from pyspark.sql.types import StructType, LongType,StringType, IntegerType, DoubleType

# create dataframe schema manually, best practices

movieSchema = StructType()\
         .add("movieId", IntegerType(), True)\
         .add("title", StringType(), True)\
         .add("genres", StringType(), True)\


ratingSchema = StructType()\
         .add("userId", IntegerType(), True)\
         .add("movieId", IntegerType(), True)\
         .add("rating", DoubleType(), True)\
         .add("timestamp", StringType(), True)


# COMMAND ----------

movieDf = spark.read.format("csv")\
          .option("header", True)\
          .schema(movieSchema)\
          .load("/FileStore/tables/movielens/movies.csv")

ratingDf = spark.read.format("csv")\
          .option("header", True)\
          .schema(ratingSchema)\
          .load("/FileStore/tables/movielens/ratings.csv")


# COMMAND ----------

movieDf.show(2)
ratingDf.show(2)

# COMMAND ----------

print("Count ", ratingDf.count())


# COMMAND ----------

# to get all columns
print("Columns", ratingDf.columns)
# schema
print(ratingDf.schema)


# COMMAND ----------

# add new columns/drive new columns from existing data
df3 = ratingDf.where("rating < 2").withColumn("rating_adjusted", ratingDf.rating + .2  )
df3.printSchema()
df3.show(2)
print("derived ", df3.count())
print("ratingDf ", ratingDf.count())


# COMMAND ----------

df2 = ratingDf.withColumnRenamed("rating", "ratings")
df2.printSchema()
df2.show(2)

# COMMAND ----------

df2 = ratingDf.select(ratingDf.userId, 
                     (ratingDf.rating + 0.2).alias("rating_adjusted") )
df2.show(1)

# COMMAND ----------

df2 = ratingDf.filter( (ratingDf.rating >=3) & (ratingDf.rating <=4))
df2.show(4)

# COMMAND ----------

from pyspark.sql.functions import col, asc, desc
# sort data by ascending order/ default
df2 = ratingDf.sort("rating")
df2.show(5)
# sort data by ascending by explitly
df2 = ratingDf.sort(asc("rating"))
df2.show(5)
# sort data by descending order
df2 = ratingDf.sort(desc("rating"))
df2.show(5)


# COMMAND ----------

# aggregation count
from pyspark.sql.functions import col, desc, avg, count
# count, groupBy
# a movie, rated by more users, dones't count avg rating
# filter, ensure that total_ratings >= 100 users
mostPopularDf = ratingDf\
                .groupBy("movieId")\
                .agg(count("userId"))\
                .withColumnRenamed("count(userId)", "total_ratings")\
                .sort(desc("total_ratings"))\
                .filter(col("total_ratings") >= 100)\
                
mostPopularDf.createOrReplaceTempView("popular_movies")
mostPopularDf.cache()
  
# logical, physical plan
mostPopularDf.explain(extended = True)
print ("--------")
# print phisical plans
mostPopularDf.explain()
#mostPopularDf.show(200)

# COMMAND ----------

df2 = spark.sql("select movieId, total_ratings from popular_movies where total_ratings > 100")
df2.show(2)

df2.explain(extended = True)

# COMMAND ----------



# COMMAND ----------

# join mostPopularmovie with movieDf, to get the title of the movie
mostPopularMoviesDf = mostPopularDf\
                      .join(movieDf, 
                            movieDf.movieId == mostPopularDf.movieId)\
                      .select(mostPopularDf.movieId, "title", "total_ratings")



mostPopularMoviesDf.show(5)


# COMMAND ----------

# perform two aggregates, count, avg, 

# aggregation of count of number of votes, +
# aggregation of avg voting
from pyspark.sql.functions import col, desc, avg, count
# count, groupBy
# a movie, rated by more users, dones't count avg rating
# filter, ensure that total_ratings >= 100 users
mostPopularDf = ratingDf\
                .groupBy("movieId")\
                .agg(count("userId").alias("total_ratings"), 
                     avg("rating").alias("avg_rating") )\
                .filter( (col("total_ratings") >= 100) &
                         (col("avg_rating") >= 3))\
                .sort(desc("total_ratings"))
                
mostPopularDf.show(200)

# COMMAND ----------

# join mostPopularmovie with movieDf, to get the title of the movie
mostPopularMoviesDf = mostPopularDf\
                      .join(movieDf, 
                            movieDf.movieId == mostPopularDf.movieId)\
                      .select(mostPopularDf.movieId, "title", "total_ratings", "avg_rating")



mostPopularMoviesDf.show()

# COMMAND ----------

print(mostPopularMoviesDf.rdd.getNumPartitions())

mostPopularMoviesDf.write.mode('overwrite')\
                         .csv("/FileStore/tables/deloitte-jan-2022/top-moives")

# COMMAND ----------

