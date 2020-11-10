from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import configparser
import time
import os
import codecs

# os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
# 1. Read here https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html that
#  hadoop-common and hadoop-aws versions had to match.
# 2. From the pyspark terminal found my hadoop version with sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
#  - It was 2.7.4 so this works below (the 2.7.3 also worked); (>2 did not work)
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.4 pyspark-shell"

config = configparser.ConfigParser()
config.read(os.path.expanduser("~/.aws/credentials"))
access_id = config.get('default', "aws_access_key_id")
access_key = config.get('default', "aws_secret_access_key")

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.access.key", access_id)
hadoop_conf.set("fs.s3a.secret.key", access_key)


def load_movie_name(name_path):
    movie_names = {}
    with codecs.open(name_path, encoding='ISO-8859-1', mode='r') as f:
        for i_line, line in enumerate(f):
            ary = line.split('::')
            movie_id = int(ary[0])
            movie_name = ary[1]
            movie_names[movie_id] = movie_name
            if i_line > 10000:
                break
    return movie_names


def load_movie_ratings(data_path):
    data = sc.textFile(data_path)
    ratings = data.map(lambda l: l.split("::")).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
    return ratings


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


def filter_diagonals(row):
    movie1 = row[1][0][0]
    movie2 = row[1][1][0]

    return movie1 < movie2


def key_by_movie_pair(row):
    user_id = row[0]
    movie_id_ratings_pair = row[1]
    movie1, rating1 = movie_id_ratings_pair[0]
    movie2, rating2 = movie_id_ratings_pair[1]
    return (movie1, movie2), (rating1, rating2)


def cosine_similarity(rating_pairs):
    xx = yy = xy = 0
    for rating_x, rating_y in rating_pairs:
        xx += rating_x**2
        yy += rating_y**2
        xy += rating_x * rating_y
    numerator = xy
    denominator = xx**0.5 * yy**0.5

    score = 0
    if denominator:
        score = numerator / float(denominator)

    return score, len(rating_pairs)


def show_partition_id(df, _part=0):
    return df.select(*df.columns, F.spark_partition_id().alias("pid")).filter(F.col("pid") == _part).show()


if __name__ == '__main__':
    t0 = time.time()
    # Load up movie names (sep | )  and broadcast it
    # movieNames = spark.sparkContext.broadcast(load_movie_name('../data/ml-100k/u.item'))
    movieNames = load_movie_name('../data/ml-1m/movies.dat')

    # Read in movie ratings
    rdd_ratings = sc.parallelize(load_movie_ratings('s3a://coleman-spark/ml-1m/ratings.dat').take(10000))
    # Partitioning should help the expensive self-join to follow
    ratingsPartitioned = rdd_ratings.partitionBy(100)

    # self-join and get an RDD with elements like (userid, ( (movie1, rating1), (movie2, rating2) ) )
    joinedRatings = ratingsPartitioned.join(ratingsPartitioned)

    # Use a function to filter the rdd removing duplicates
    uniqueJoinedRatings = joinedRatings.filter(filter_diagonals)

    # Key by (movie1, movie2) now instead
    moviePairs = uniqueJoinedRatings.map(key_by_movie_pair)

    print(moviePairs.count())

    # https://www.oreilly.com/library/view/learning-spark/9781449359034/ch04.html
    # groupByKey() creates another RDD with all the values grouped to same key in a list
    # So then the mapValues will apply func to a list
    similarScores = moviePairs.groupByKey().mapValues(cosine_similarity)

    similarScores = similarScores.sortBy(lambda x: -1*x[1][1])

    spark = SparkSession(sc)
    similarScores = similarScores.map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1])).\
        toDF(["movie1", "movie2", "score", "numPairs"])
    print(similarScores.rdd.getNumPartitions())
    similarScores.show(10, False)
    show_partition_id(similarScores, 1)

    # tmp = similarScores.take(10)
    # print(type(tmp))
    # for i, t in enumerate(tmp):
    #     if i == 0:
    #         print(type(t))
    #     print(t)

    # tf = time.time()
    # print(f'Finished program in {tf - t0} seconds')
