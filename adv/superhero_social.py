from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
import codecs


def parse_connect_line(_line):
    ary = _line.split()
    _id = int(ary[0])
    # Make sure this character has _any_ friends
    if len(ary) < 2:
        _connects = 0
    else:
        _connects = int(ary[1])
    return _id, _connects


def parse_name_line(_line):
    ary = _line.split()
    _id = int(ary[0])
    _name = " ".join(ary[1:])
    _name = _name[1:-1]
    return _id, _name


def load_hero_names():
    hero_names = {}
    with codecs.open('../data/Marvel+Names', 'r', encoding="ISO-8859-1", errors='ignore') as f:
        for line in f:
            ary = line.split()
            _id = int(ary[0])
            _name = " ".join(ary[1:])
            _name = _name[1:-1]
            hero_names[_id] = _name
    return hero_names


# Boilerplate app name and log level
spark = SparkSession.builder.appName("PopularMarvel").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Create broadcast object from the superhero names
name_dict = spark.sparkContext.broadcast(load_hero_names())

# Read text file in and create RDD
lines = spark.sparkContext.textFile("../data/Marvel+Graph")
rdd = lines.map(parse_connect_line)

print(rdd.take(5))

# Define df schema of ID + number of connections
schema = StructType([
    StructField("heroID", IntegerType(), False),
    StructField("connectCount", IntegerType(), True)
])

# Convert rdd into a df of that schema
df = spark.createDataFrame(data=rdd, schema=schema)
df.show(5, False)


# Map hero's name using a UDF with the broadcast dict
@udf
def get_hero_name(_id):
    return name_dict.value[_id]


# Deduplicate the heroID by grouping by it and summing
df = df.groupBy("heroID").agg(F.sum(F.col("connectCount")).alias('connections'))

# Use the UDF to populate the name column
df = df.withColumn("heroName", get_hero_name(F.col("heroID")))
df.show(5, False)

# Let's try a join to make sure we get the same result
name_schema = StructType([
    StructField("heroID", IntegerType(), False),
    StructField("heroName", StringType(), False)
])

# Read into an rdd applying a map function to decode iso-8859-1, then parsing to get the quotes gone
rdd_names = spark.sparkContext.textFile('../data/Marvel+Names', use_unicode=False).map(lambda x: x.decode("iso-8859-1"))
rdd_names = rdd_names.map(parse_name_line)

# Instead of this rdd business again, we could have read in the entire name for a column with quotes and then used
# the substring method

# Create dataframe from rdd
df_names = spark.createDataFrame(data=rdd_names, schema=name_schema)

df_names.show(5)

# Join and see that you have the same names
df = df.join(df_names.withColumnRenamed('heroName', 'heroName2'), on='heroID', how='left')

# Order for popularity
df = df.orderBy(F.desc('connections'))

df.show(10, False)




