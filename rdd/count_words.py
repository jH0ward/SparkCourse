import re
import time
from pyspark import SparkConf, SparkContext
from functools import cmp_to_key

FANCY = False


def normalize_words(w):
    return re.compile(r'\W+', re.UNICODE).split(w.lower())


conf = SparkConf().setMaster("local[*]").setAppName("RDDIntro")
sc = SparkContext(conf=conf)
sc.setLogLevel('ERROR')
t0 = time.time()
book = sc.textFile('../data/Book.txt')

if FANCY:
    words = book.flatMap(normalize_words)
    # The hard way to countByValue
    wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

    # This is what Frank did
    # wordCounts = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
    # wordCounts = wordCounts.collect()

    # Can do this instead
    wordCounts = wordCounts.sortBy(lambda x: x[1]).collect()

    for word, c in wordCounts:
        cleaned = word.encode('ascii', 'ignore')
        if cleaned:
            print(cleaned.decode('utf-8'), ': ', c)

else:
    words = book.flatMap(lambda x: x.split())
    wordCounts = words.countByValue()
    print(type(wordCounts))
    # print(wordCounts)
    # This is sorted the dict desc the hard way, with the cmp_to_key function
    # wordCounts = sorted(wordCounts.items(), key=cmp_to_key(lambda kv1, kv2:
    #                                                        kv2[1] - kv1[1]))

    # Easier way
    wordCounts = sorted(wordCounts.items(), key=lambda wc: wc[1], reverse=True)

    for word, c in wordCounts:
        cleaned = word.encode('ascii', 'ignore')
        if cleaned:
            print(cleaned.decode('utf-8'), ': ', c)

    print('book')
    print(book.take(5))
    print(words.take(5))

print(f'took {time.time() - t0}')
