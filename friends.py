from pyspark import SparkConf, SparkContext


def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return age, num_friends


conf = SparkConf().setMaster("local[*]").setAppName("RDDIntro")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')

lines = sc.textFile('./fakefriends.csv')

rdd = lines.map(parse_line)

# rdd ~ [(33, 385), (26, 2), ...]
print(rdd.take(2))

# first mapValues ~ (33, (385, 1)) x = 385
# reduceByKey ~ (33, (<sum of friend counts ppl aged 33>, <count of people aged 33>)

totals_by_age = rdd.mapValues(lambda x: (x, 1)).\
    reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# mapValues x[0] / x1 takes each age and does value[0] (total) / count of that age
avgs_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])
avgs_by_age = avgs_by_age.sortByKey(ascending=True)
print(avgs_by_age.collect())

# alternative strategy from https://stackoverflow.com/a/29930162
# aggreateByKey takes 2 functions - first is within-partition reduction, which is
# a tuple of (running sum, count in partition) and the second func is cross-partition
# which does sum of aggregated age-33 friend counts across partitions for ex.
# b/c age33 people key could be on multiple partitions
alt_rdd = rdd.aggregateByKey((0, 0), lambda a, b: (a[0] + b, a[1] + 1),
                             lambda a, b: (a[0] + b[0], a[1] + b[1]))

alt_final = alt_rdd.mapValues(lambda v: v[0]/v[1])
print(alt_final.sortByKey(ascending=True).collect())
