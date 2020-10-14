from pyspark import SparkConf, SparkContext

RUN_MAX = True


def parse_csv_lines(line):
    ary = line.split(',')
    station_id = ary[0]
    entry_type = ary[2]
    temp = float(ary[3]) * .1 * 9./5 + 32.
    return station_id, entry_type, temp


conf = SparkConf().setMaster("local[*]").setAppName("RDDIntro")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')

lines = sc.textFile('./1800.csv')

rdd = lines.map(parse_csv_lines)

# Only keep lines reporting minimum temperature
if RUN_MAX:
    rdd = rdd.filter(lambda x: 'TMAX' in x[1])
else:
    rdd = rdd.filter(lambda x: 'TMIN' in x[1])

# Print all the station IDs present
print(rdd.keys().distinct().collect())

# Now only keep the station_id and the temp value
rdd = rdd.map(lambda x: (x[0], x[2]))

print(rdd.countByKey())

# Reduce by station_id to keep only the min for each key
if RUN_MAX:
    rdd = rdd.reduceByKey(lambda x, y: max(x, y))
else:
    rdd = rdd.reduceByKey(lambda x, y: min(x, y))

print(rdd.collect())
