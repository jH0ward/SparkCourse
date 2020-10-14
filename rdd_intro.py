from pyspark import SparkConf, SparkContext
import time

conf = SparkConf().setMaster("local[*]").setAppName("RDDIntro")
sc = SparkContext(conf=conf)
print(sc.getConf().getAll())
print("cores = ", conf.get('spark.executor.cores'))

n = int(1e6)
print(f'n*(n+1)(2n+1)/6 = {n * (n+1) * (2*n+1) / 6.0}')

t0 = time.time()
rdd = sc.parallelize(list(range(n)))
res = rdd.map(lambda x: x**2)
print(res.sum())
t1 = time.time()
print(f'Spark sum of {n} squares took {t1-t0} seconds')

t0 = time.time()
ary = list(range(n))
tot = 0.
a2 = [a**2 for a in ary]
print(f'sum of py list = {sum(a2)}')
t1 = time.time()
print(f'Python sum of {n} squares took {t1-t0} seconds')



