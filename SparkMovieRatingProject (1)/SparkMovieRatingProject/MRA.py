import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window



def read_files(spark):
    movies_df  = spark.read.format("CSV").option("inferSchema","True").option('header','True').load("C:\\Users\\asus\\PycharmProjects\\pythonProject\\SparkMovieRatingProject (1)\\SparkMovieRatingProject\movies.csv")
    #movies_df.show()

    ratings_df = spark.read.format("CSV").option("inferSchema","True").option("header","True").load("C:\\Users\\asus\\PycharmProjects\\pythonProject\\SparkMovieRatingProject (1)\\SparkMovieRatingProject\\ratings.csv")
    #ratings_df.show()

    tags_df = spark.read.format("CSV").option("inferSchema", "True").option('header', 'True').load("C:\\Users\\asus\\PycharmProjects\\pythonProject\\SparkMovieRatingProject (1)\\SparkMovieRatingProject\\tags.csv")
    #tags_df.show()

    return movies_df, ratings_df, tags_df

#a
def agg_no_of_ratings_per_year(ratings_df):
     ratings_df1 = ratings_df.withColumn("timestamp",from_unixtime(col("timestamp")))
     #ratings_df1.show(10)
     agg_ratings = ratings_df1.groupBy(substring("timestamp",0,4).alias("Year")) \
                    .agg(count("*").alias("count"))
     #agg_ratings.show()
     return ratings_df1

def avg_monthly_rating(ratings_df1):
    avg_rating = ratings_df1.groupBy(substring("timestamp",6,2).alias("Month"))\
                .agg(count("rating").alias("Average count"))\
                .orderBy("Month")
    avg_rating.show()

def ratings_level_distribution(ratings_df1):
    ratings = ratings_df1.groupBy(col("rating").alias("rating_given"))\
        .agg(count("*").alias("count_ratings"))\
        .orderBy("rating_given")
    ratings.show()

def movies_tagged_but_not_rated(movies_df,ratings_df1,tags_df):
    tags_df1 = tags_df.withColumn("timestamp", from_unixtime(col("timestamp")))
    #tags_df1.show(10)

    tagged_not_rated = movies_df.join(tags_df, movies_df.movieId == tags_df.movieId, 'inner')\
                        .join(ratings_df1, movies_df.movieId == ratings_df1.movieId, 'left' )\
                        .filter(col("rating").isNull())\
                        .select("title").distinct()

    #tagged_not_rated.show()
    return tags_df1

def movies_rated_but_not_tagged(movies_df,tags_df1,ratings_df1):
    rated_not_tagged = movies_df.join(ratings_df1, ratings_df1.movieId == movies_df.movieId, 'inner') \
                        .join(tags_df1, tags_df1.movieId == movies_df.movieId, 'left') \
                        .filter(col("tag").isNull())


    #print(rated_not_tagged.count())
    #rated_not_tagged.select("title").distinct().show()
    return rated_not_tagged

def top_10_rated_not_tagged(movies_df,rated_not_tagged):
    top_10 = rated_not_tagged.groupby(movies_df.movieId,"title")\
            .agg(avg("rating").alias("avg_rating"),
                 count("rating").alias("count_rating"))\
            .orderBy(col("avg_rating").desc(),col("count_rating").desc())

    top_10.show(10)

def g_func(tags_df):
    tags = tags_df.agg(count("tag").alias("count_tag")).collect()[0]["count_tag"]
    print(tags)

    no_of_movies = tags_df.select("movieId").distinct().count()
    print(no_of_movies)

    tags_per_movies = tags/no_of_movies
    print(int(tags_per_movies))


def h_func(tags_df1):
    tags_df1.select("userId").distinct().orderBy("userId").show()

def i_func(ratings_df):
    count_ratings = ratings_df.agg(count("rating").alias("count_rating")).collect()[0]['count_rating']
    print(count_ratings)
    total_users = ratings_df.select("userId").distinct().count()
    print(total_users)

    avg_ratings_per_user = count_ratings / total_users
    print(avg_ratings_per_user)

def j_func(movies_df, ratings_df):
    predominent_genres_per_rating = movies_df.join(ratings_df, movies_df.movieId ==  ratings_df.movieId,'inner')
    #predominent_genres_per_rating.show()
    explode_df = predominent_genres_per_rating.withColumn("genres",explode(split("genres","\|")))
    grouped_df = explode_df.groupby("genres","rating").count()
    grouped_df.show()
    window = Window.partitionBy("rating").orderBy(col("count").desc())
    ranked_df = grouped_df.withColumn("rank", rank().over(window)).filter(col("rank") == 1).orderBy(col("rating").desc())
    ranked_df.select("rating", "genre").show()


#k. What is the predominant tag per genre and the most tagged genres?
def k_func(movies_df,tags_df):
    joined_df = movies_df.join(tags_df, movies_df.movieId == tags_df.movieId, 'inner')
    #joined_df.show()
    exploded_df = joined_df.withColumn("genres",explode(split("genres","\|")))
    #exploded_df.show()
    grouped_df = exploded_df.groupBy("genres","tag").count()
    grouped_df.show()
    window = Window.partitionBy("genres").orderBy(col("count").desc())
    ranked_df = grouped_df.withColumn("rank",rank().over(window)).filter(col('rank')==1).orderBy(col("genres"))
    ranked_df.show()


def main():
    spark = SparkSession.builder.appName("MovieRatingProject").getOrCreate()
    movies_df, ratings_df, tags_df = read_files(spark)
    ratings_df1 = agg_no_of_ratings_per_year(ratings_df)
    #avg_monthly_rating(ratings_df1)
    #ratings_level_distribution(ratings_df1)
    tags_df1 = movies_tagged_but_not_rated(movies_df, ratings_df1, tags_df)
    #rated_not_tagged = movies_rated_but_not_tagged(movies_df,tags_df1,ratings_df1)
    #top_10_rated_not_tagged(movies_df,rated_not_tagged)
    #g_func(tags_df)
    #h_func(tags_df1)
    #i_func(ratings_df)
    #j_func(movies_df, ratings_df)
    k_func(movies_df,tags_df)
    return spark



if __name__=="__main__":
    spark = main()
    spark.stop()











