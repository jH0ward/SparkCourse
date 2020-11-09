from pyspark import SparkConf, SparkContext
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
print(access_id)
access_key = config.get('default', "aws_secret_access_key")

conf = SparkConf()
sc = SparkContext(conf=conf)
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


if __name__ == '__main__':
    t0 = time.time()
    # Load up movie names (sep | )  and broadcast it
    # movieNames = spark.sparkContext.broadcast(load_movie_name('../data/ml-100k/u.item'))
    movieNames = load_movie_name('../data/ml-1m/movies.dat')

    # Read in movie ratings
    # Load up movie data (sep \t)
    rdd_ratings = load_movie_ratings('s3a://coleman-spark/ml-1m/ratings.dat')
    ratingsPartitioned = rdd_ratings.partitionBy(100)
    joinedRatings = ratingsPartitioned.join(ratingsPartitioned)
    print(joinedRatings.take(10))

    tf = time.time()
    print(f'Finished program in {tf - t0} seconds')
