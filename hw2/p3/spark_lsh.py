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
R = 10
B = 9
RB = R * B

spark = (
    SparkSession.builder
        .master('local[*]')
        .appName('LSH app')
        .getOrCreate()
)

document = (
    spark.read.csv('../data/clean_file.tsv', sep = '\t')
    # [field1, field2, ..., fieldN]
)

description = (
    document.rdd\
        .zipWithIndex()
        # ([field1, field2, ..., fieldN] , line_num)
        .repartition(4)
        .map(lambda e: (e[1], e[0][DESCRIPTION_INDEX]))
        # (line_num, description)
)

shingles = (
    description.mapValues(shinglerFun(SHINGLE_SIZE))
    # (line_num, {shingle1, shingle2, ..., shingleN})
)

hashedShingles = (
    shingles
        .mapValues(lambda shs:
                    set([murmurHash(0)(s) for s in shs]))
        # (line_num, {h(shingle1), h(shingle2), ..., h(shingleN)})
)

minWiseHash = (
    hashedShingles
        .mapValues(lambda s: [min([murmurHash(i)(str(e)) for e in s ])
                              for i in range(1, RB + 1) ])
        # (line_num, [h_min1(shingles), h_min2(shingles), ...,
        #             h_min{R*B}(shingles)])
        # (line_num, sketch)
)

lsh = (
    minWiseHash
        .flatMap(lambda e: [( (
                                i,
                                murmurHash(0)(str(subs(e[1], i, R)))
                              ),
                              [(e[0], e[1])]
                            )
                            for i in range(B)])
        # ( (band_id, sketch[:]), [(line_num, sketch)] )
)

def combinate(e):
    l = e[1]
    res = []
    for i in range(len(l)):
        for j in range(i + 1, len(l)):
            if l[i][0] < l[j][0]:
                res.append( ( (l[i][0], l[j][0]), (l[i][1], l[j][1]) ) )
            else:
                res.append( ( (l[j][0], l[i][0]), (l[j][1], l[i][1]) ) )
    return res


similar = (
    lsh
        .reduceByKey(lambda d1, d2: d1 + d2)
        # ( (band_id, sketch[:]), [(line_num, sketch),...] )
        .flatMap(combinate)
        # ( (line_num1, line_num2), (sketch1, sketch2) )
)

def similarity_above(thresh):
    def resFun(docs):
        v1 = docs[1][0]
        v2 = docs[1][1]
        return (sum([1 for i in range(len(v1)) if v1[i] == v2[i]])
                /len(v1) >= thresh)
    return resFun

similar = (
    similar
        .reduceByKey(lambda e1, e2: e1)
        # Removes duplicates created from different buckets
        .filter(similarity_above(0.8))
)

print('LSH: {} duplicates found'.format(similar.count()))
exit()

def jaccard_similarity(docs):
    return (docs[0][0], docs[1][0],
            len(docs[0][1] & docs[1][1]) / len(docs[0][1] | docs[1][1]))


docsSimilarities = hashedShingles\
            .cartesian(hashedShingles)\
            .filter(lambda e: e[0][0] > e[1][0])\
            .map(jaccard_similarity)\

similarDocs = docsSimilarities.filter(lambda e: e[2] > 0.8)

#print(similarDocs.count())
