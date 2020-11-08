from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, LongType
from pyspark.sql import functions as F
import time
import codecs

spark = SparkSession.builder.appName("CollabFiltering").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def load_movie_name(name_path):
    movie_names = {}
    with codecs.open(name_path, encoding='ISO-8859-1', mode='r') as f:
        for line in f:
            ary = line.split('|')
            movie_id = int(ary[0])
            movie_name = ary[1]
            movie_names[movie_id] = movie_name
    return movie_names


def load_movie_ratings(data_path):
    schema = StructType([
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True)
    ])
    return spark.read.option("sep", "\t").schema(schema).csv(data_path).select("userID", "movieID", "rating")


def compute_similarity(df):
    df = df.withColumn('xx', F.col('rating_1') * F.col('rating_1')).\
        withColumn('yy', F.col('rating_2') * F.col('rating_2')).\
        withColumn('xy', F.col('rating_1') * F.col('rating_2'))
    # df.show(5, False)
    calcSim = df.groupBy('movieID_1', 'movieID_2').agg(
        F.sum(F.col('xy')).alias('numerator'),
        (F.sqrt(F.sum(F.col('xx'))) * F.sqrt(F.sum(F.col('yy')))).alias('denominator'),
        F.count(F.col('xy')).alias('numPairs')
    )
    results = calcSim.withColumn("Score", F.col('numerator') / F.col('denominator'))

    return results.select('movieID_1', 'movieID_2', 'Score', 'numPairs')


if __name__ == '__main__':
    t0 = time.time()
    # Load up movie names (sep | )  and broadcast it
    movieNames = spark.sparkContext.broadcast(load_movie_name('../data/ml-100k/u.item'))

    # Read in movie ratings
    # Load up movie data (sep \t)
    movieRatings = load_movie_ratings('../data/ml-100k/u.data')

    # Self-join to find each movies pair (only unique)
    # End up with columns of movie1, movie2, rating1, rating2
    pairRatings = movieRatings.alias('ratings1').join(
        movieRatings.alias('ratings2'),
        on=(F.col('ratings1.userID') == F.col('ratings2.userID')) &
           (F.col('ratings1.movieID') > F.col('ratings2.movieID'))
    ).select(
        F.col('ratings1.userID').alias('userID'),
        F.col('ratings1.movieID').alias('movieID_1'),
        F.col('ratings2.movieID').alias('movieID_2'),
        F.col('ratings1.rating').alias('rating_1'),
        F.col('ratings2.rating').alias('rating_2')
    )
    pairRatings.printSchema()
    # pairRatings.orderBy(F.desc('userID')).show(10, False)

    # Compute movie pair similarities from cosine similarity function
    similarDF = compute_similarity(pairRatings).cache()
    similarDF.show(5, False)
    # Returns df with cols movie1, movie2, score, numPairs

    movie_ref = 50
    print(f'Finding movies similar to {movieNames.value[movie_ref]}')
    similarDF = similarDF.filter(
        (F.col('movieID_1') == movie_ref) |
        (F.col('movieID_2') == movie_ref)
    )
    similarDF = similarDF.filter(F.col('Score') > 0.95)
    similarDF = similarDF.filter(F.col('numPairs') > 50)
    results = similarDF.orderBy(F.desc('Score')).take(10)
    print(type(results))
    for result in results:
        other_movie = result.movieID_1
        if other_movie == movie_ref:
            other_movie = result.movieID_2
        print(f'Similar movie (score = {result.Score}): {other_movie}: {movieNames.value[other_movie]}')

    tf = time.time()
    print(f'Finished program in {tf - t0} seconds')
