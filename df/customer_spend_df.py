from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Create spark session (defaults visible with sc.getConf().getAll()) - can specify master or not
spark = SparkSession.builder.appName("CustomerSpendDF").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema
schema = StructType([
    StructField("customer_id", StringType()),
    StructField("purchase_id", StringType()),
    StructField("purchase_amount", FloatType())
])

df = spark.read.schema(schema).csv("../data/customer-orders.csv")

df.printSchema()

# limit to 2 relevant columns
df = df.select('customer_id', 'purchase_amount')

df.show(5)

# Do calc
df = df.groupby('customer_id').agg(F.sum(F.col('purchase_amount')).alias('total_purchases'))

# Round to 2 decimal places
df = df.withColumn('total_purchases', F.round(F.col('total_purchases'), 2))

# Order by biggest customers
df = df.orderBy(F.col('total_purchases').desc())

df.show(5)

spark.stop()
