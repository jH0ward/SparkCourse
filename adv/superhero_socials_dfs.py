from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# Boilerplate spark session and logger
spark = SparkSession.builder.appName("PopSuperheroDFs").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read in text file to dataframe
lines = spark.read.text("../data/Marvel+Graph")

# Split value column to get heroID
df = lines.\
    withColumn("heroID", F.split(F.col("value"), ' ')[0]).\
    withColumn("count", F.size(F.split(lines.value, " ")) - 1).\
    select("heroID", "count")

# Deduplicate the heroID that required multiple rows for their networks
df = df.groupBy("heroID").agg(F.sum("count").alias("connections"))

# Define schema for hero names and load them in new dataframe
name_schema = StructType([
    StructField("heroID", IntegerType(), False),
    StructField("heroName", StringType(), False)
])

df_names = spark.read.csv("../data/Marvel+Names", sep=" ", schema=name_schema)

# Left join ID/connect df on to the names
df = df.join(df_names, how='left', on='heroID')
df = df.orderBy(F.desc("connections"))
df.show(10, False)

# This grabs the top row
mostPopular = df.first()

print(f'Most popular superhero is {mostPopular.heroName}. They have {mostPopular["connections"]} connections')

print(f'Anybody with zero conns?')
df.filter(F.col('connections') < 1).show()

# Get python list of all less than 2
few_friends = list(df.filter(F.col('connections') < 2).select('heroName').toPandas()['heroName'])
print(few_friends)

for f in few_friends:
    print(f)

# Try brute-force min calc on df
print(df.groupby().agg(F.min(F.col("connections"))).first()[0])
# So then you could filter on condition col(connect..) = min_val from above instead of < 2
# (they could have been 3 for ex. instead of 1)

# We could have also used this to make sure there wasn't a single unique most popular hero (instead of Cap america)

spark.stop()
