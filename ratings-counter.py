from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local[*]").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

sc.setLogLevel('ERROR')

lines = sc.textFile("../SparkCourse/ml-100k/u.data")
first_five = lines.take(5)
print(repr(first_five[0]))
for lin in first_five:
    print(str(lin))


ratings = lines.map(lambda x: x.split()[2])
n_ratings = lines.count()
print(f'Got {n_ratings} ratings')
result = ratings.countByValue()
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

