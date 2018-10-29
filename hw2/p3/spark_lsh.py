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
    return (docs[0][0], docs[1][0], len(docs[0][1] & docs[1][1]) / len(docs[0][1] | docs[1][1]))


docsSimilarities = hashedShingles\
            .cartesian(hashedShingles)\
            .filter(lambda e: e[0][0] > e[1][0])\
            .map(jaccard_similarity)\

similarDocs = docsSimilarities.filter(lambda e: e[2] > 0.8)

#print(similarDocs.count())

def similarity_above_threshold(doc):
    v1 = doc[1][0][1]
    v2 = doc[1][1]
    return sum([1 for i in range(len(v1)) if v1[i] == v2[i]])/len(v1) >= 0.8

similar = similar.join(minWiseHash)\
            .map(lambda e: (e[1][0],(e[0], e[1][1]) ))\
            .join(minWiseHash)\
            .filter(similarity_above_threshold)


print("LSH similar items", similar.count())