from pyspark import SparkConf, SparkContext


def parse_line(line):
    fields = line.split(',')
    customer_id = int(fields[0])
    spend = float(fields[2])
    return customer_id, spend


def flip(t):
    return t[1], t[0]


# Boilerplate spark setup
conf = SparkConf().setMaster("local[*]").setAppName("SpendByCustomer")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')

# Read in text for initial RDD
lines = sc.textFile('./customer-orders.csv')

# Map text RDD to pair RDD
rdd = lines.map(parse_line)
print(rdd.take(3))

# Reduce with lambda function
rdd_totals = rdd.reduceByKey(lambda x, y: x + y)

# Sort highest total to lowest
rdd_totals = rdd_totals.sortBy(lambda x: x[1], ascending=False)
print(rdd_totals.collect())

# Alternate method is to flip key, val and order by Key
rdd_totals = rdd_totals.map(flip).\
    sortByKey(ascending=False).\
    map(flip)

py_obj = rdd_totals.collect()

print(py_obj)

# Type is list of tuples
print(type(py_obj))

