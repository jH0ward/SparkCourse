from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

# Will demo broadcasting a relatively small piece of data to each executor for convenient lookup
# by making a broadcast object from a dict and getting its values by a UDF


def load_movie_names():
    # maps integer movie ids to string movie names
    movie_names = {}
    with codecs.open("../data/ml-100k/u.item", "r", encoding="ISO-8859-1", errors='ignore') as f:
        for line in f:
            ary = line.split('|')
            movie_names[int(ary[0])] = ary[1]
    return movie_names


spark = SparkSession.builder.appName("BcastMovies").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

name_dict = spark.sparkContext.broadcast(load_movie_names())


schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Read in the u.data into df
df = spark.read.option("sep", "\t").schema(schema).csv("../data/ml-100k/u.data")

# Get counts by movie id
movie_counts = df.groupby("movieID").count()


# Create UDF to get name from id
def get_name_from_id(_id):
    aVar = 1
    return name_dict.value[_id]


lookupNameUDF = F.udf(get_name_from_id)

# Create movie name column movieTitle with contents from UDF
moviesWithNames = movie_counts.withColumn("movieTitle", lookupNameUDF(F.col("movieID")))


# Order by popularity
moviesWithNames = moviesWithNames.orderBy(F.desc("count"))

moviesWithNames.show(10, False)

spark.stop()
