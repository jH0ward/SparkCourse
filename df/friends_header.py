from pyspark.sql import SparkSession
from pyspark.sql import Row


# Boilerplate for Spark session builder
# ex. here https://dzone.com/articles/introduction-to-spark-session
spark = SparkSession.builder. \
    master("local"). \
    appName("SparkSQL"). \
    getOrCreate()


# Try to set log level
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Use sparkContext to read into rdd
people = spark.read.\
    option("header", "true").\
    option("inferSchema", "true")\
    .csv("../data/fakefriends-header.csv")

print(type(people))
people.printSchema()
print(people.take(2))

# Do some sql
# select a col
people.select("name").show()

# filter people over 21
people.filter(people.age < 21).show()

# groupby age and count group size
people.groupBy("age").count().show()

# make a new column of new ages
people.select(people.name, people.age + 10).show()


spark.stop()



