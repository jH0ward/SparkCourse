from pyspark.sql import SparkSession
from pyspark.sql import Row


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]),
               name=str(fields[1].encode("utf-8")),
               age=int(fields[2]),
               numFriends=int(fields[3]),
               )


# Boilerplate for Spark session builder
# ex. here https://dzone.com/articles/introduction-to-spark-session
spark = SparkSession.builder.\
    master("local").\
    appName("SparkSQL").\
    getOrCreate()


# Try to set log level
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Use sparkContext to read into rdd
lines = spark.sparkContext.textFile("../data/fakefriends.csv")
print(lines.take(2))

# Map to second RDD of Row Objects
people = lines.map(mapper)
print(people.take(3))

# Create a DataFrame from the people rdd of rows
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.show(5)

# Register a table view and run SQL on it
schemaPeople.createOrReplaceTempView("people")
teens = spark.sql('SELECT * FROM people where age < 20 and age > 12')
teens.show(5)

# Collect on the df will return elements as array
for t in teens.collect():
    print(t)

schemaPeople.groupby("age").count().orderBy("age").show()

# Stop/close the connection
spark.stop()
