from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read text file into a one-column dataframe
inputDF = spark.read.text('../data/Book.txt')
inputDF.show()

# Split text by word and put into new dataframe with col named "word"
words = inputDF.select(
    F.explode(F.split(inputDF.value, "\\W+"))\
    .alias("word")
)

words = words.filter(words.word != "")
# 2 ways to make word column lower case, withColumn or complicated select with alias call
# words = words.withColumn("word", F.lower(F.col("word")))
words = words.select(F.lower(F.col("word")).alias("word"))

words.groupBy("word").count().sort(F.col("count").desc()).show()

print(words.count())


