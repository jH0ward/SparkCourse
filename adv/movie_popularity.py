from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Load up movie data as df using tab separator
df = spark.read.option("sep", '\t').schema(schema).csv("../data/ml-100k/u.data")

df.printSchema()

# Only really need this col to count number of reviews for each (b/c its number of rows)
df = df.select('movieID')

# Groupby and count that single column
# df = df.groupby('movieID').count().orderBy(F.col('count').desc())

# Apparently there's a desc function
df = df.groupby('movieID').count().orderBy(F.desc('count'))

# Show the top 10 most popular
df.show(10)

spark.stop()
