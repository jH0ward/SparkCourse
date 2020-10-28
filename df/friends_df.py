from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.\
    master("local").\
    appName("FriendsDF").\
    getOrCreate()


sc = spark.sparkContext
sc.setLogLevel("ERROR")

df = spark.read.\
    option("header", "true").\
    option("inferSchema", "true").\
    csv("../data/fakefriends-header.csv")


print(type(df))

df.printSchema()

print(df.take(2))

# df_avg_friends = df.select("age", "friends").groupBy("age").avg("friends")
df_avg_friends = df.select("age", "friends").groupBy("age").agg(F.avg("friends").
                                                                alias("friends_avg"))
df_avg_friends = df_avg_friends.orderBy('age')
df_avg_friends.show()

# df.groupby("age").agg(F.round(F.avg("friends"), 2)).show()
df_avg_friends.withColumn("friends_avg", F.round(F.col("friends_avg"), 2)).show()

#df_avg_friends.withColumn("friends_")
spark.stop()


