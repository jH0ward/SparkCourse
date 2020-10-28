from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, StructField

spark = SparkSession.builder.appName("MinTemp").getOrCreate()

# Define the schema of the csv file to be read in by data types
schema = StructType([
   StructField("stationID", StringType(), True),
   StructField("date", IntegerType(), True),
   StructField("measure_type", StringType(), True),
   StructField("temperature", FloatType(), True)
])

df = spark.read.schema(schema).csv("../data/1800.csv")
df.printSchema()
df.show(3)

# Filter only the min temp rows
df_min = df.filter(F.col("measure_type") == 'TMIN').select("stationID", "temperature")

# Group and compute the min temp for each stationID
df_station_min = df_min.groupBy("stationID").agg(F.min("temperature").alias("min_temp"))

# Make a new column (demonstrate withColumn and use the round Function as well; Could do an alias instead
# df_station_min = df_station_min.withColumn("temp_F", F.round(F.col("min_temp") * .1 * 9./5 + 32, 2))
df_station_min = df_station_min.withColumn("temp_F", F.round(F.col("min_temp") * .1 * 9./5 + 32, 2))

df_station_min.show()

spark.stop()


