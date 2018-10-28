#!/usr/bin/env python3

try:
    import mmh3 as mmh3
except ImportError:
    import pymmh3 as mmh3

from pyspark.sql import SparkSession
from itertools import combinations

def shinglerFun(k):
    return lambda d: set([d[i:i+k] for i in range(len(d) - k + 1)])

def murmurHash(seed):
    def resFun(s):
        return mmh3.hash128(s, seed)
    return resFun

def subs(e, i, r):
    """Returns the i-th subsequence of size r in a list
    
    Arguments:
        i {[type]} -- [description]
        r {[type]} -- [description]
    """
    return e[ i * r: (i + 1) * r ]


SHINGLE_SIZE = 10
DESCRIPTION_INDEX = 2
R = 5
B = 2
RB = R * B

spark = SparkSession.builder\
            .master('local[*]')\
            .appName('LSH app')\
            .getOrCreate()

document = spark.read.csv('../data/clean_file.tsv', sep = '\t')

description = document.rdd\
                .zipWithIndex()\
                .repartition(4)\
                .map(lambda e: (e[1], e[0][DESCRIPTION_INDEX]))

shingles = description.mapValues(shinglerFun(SHINGLE_SIZE))

hashedShingles = shingles\
                    .mapValues(lambda shs: \
                        set([murmurHash(0)(s) for s in shs]))

minWiseHash = hashedShingles\
                    .mapValues(lambda s: [min([murmurHash(i)(str(e))
                                               for e in s ])
                                          for i in range(1, RB + 1) ])

lsh = minWiseHash.flatMap(lambda e: [( (i,
                                        murmurHash(0)(str(subs(e[1], i, R)))),
                                       e[0])
                                    for i in range(B)])

similar = lsh.groupByKey()\
             .flatMap(lambda t:
                list(combinations(sorted(list(t[1])), 2)))\
             .distinct()

def jaccard_similarity(docs):
    id1 = docs[0][0] if docs[0][0] < docs[1][0] else docs[1][0]
    id2 = docs[0][0] + docs[1][0] - id1
    return (id1, id2, len(docs[0][1] & docs[1][1]) / len(docs[0][1] | docs[1][1]))


cartProd = shingles\
            .cartesian(shingles)\
            .map(jaccard_similarity)


print(cartProd.take(10))

# print("LSH similar items", similar.count())
