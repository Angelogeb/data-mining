#!/usr/bin/env python3

import itertools
import pickle
import os
import argparse
import time

try:
    import mmh3 as mmh3
except ImportError:
    import pymmh3 as mmh3

from collections import namedtuple

parser = argparse.ArgumentParser(prog='LSH near duplicate detection', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('file')

parser.add_argument('-k',
                    type = int,
                    default = 10,
                    help = 'Sets the size of the k-grams')

parser.add_argument('-r',
                    type = int,
                    default = 10,
                    help = 'Length of one band')

parser.add_argument('-b',
                    type = int,
                    default = 9,
                    help = 'Number of bands')

parser.add_argument('-t',
                    type = float,
                    default = 0.8,
                    help = 'Jaccard similarity threshold')

parser.add_argument('-i',
                    type = int,
                    default = 2,
                    help = 'Index of the document column in the tsv')

parser.add_argument('--skip-jaccard',
                    '-s',
                    action = 'store_true',
                    help = 'Skips computing the jaccard similarity between\
                            documents')

parser.add_argument('-f',
                    action = 'store_true',
                    help = 'Perform postfiltering after LSH')

def murmurHash(seed):
    def resFun(s):
        return mmh3.hash128(s, seed)
    return resFun

class ShinglesVectorizer:
    def __init__(self, k = 10):
        self.k = k
    
    def transform(self, documents):
        if type(documents) != list: documents = [documents]
        assert (len(documents[0]) - self.k + 1 > 0), "k should be less than \
                                                      the document length"
        return [set([d[i:i+self.k] for i in range(len(d) - self.k + 1)])
                for d in documents]
    
    def transformHashed(self, documents, hash_fun = mmh3.hash128):
        if type(documents) != list: documents = [documents]

        assert (len(documents[0]) - self.k + 1 > 0), "k should be less than \
                                                      the document length"
        return [set([ hash_fun(d[i:i+self.k])
                  for i in range(len(d) - self.k + 1)])
                  for d in documents ]

class MinwiseHasher:
    def __init__(self, rb = 100):
        self.rb = rb
    
    def transform(self, sets):
        res = []
        for s in sets:
            sig = [ min([ murmurHash(i)(str(e)) for e in s ])
                    for i in range(1, self.rb + 1) ]
            res.append(sig)
        return res

SimilarPair = namedtuple('SimilarPair', ['idx1', 'idx2', 'score'])

class LocalitySensitiveHashing:
    def __init__(self, r = 10):
        self.r = r

    def transform(self, signatures, similarity_threshold = 0):
        assert (len(signatures[0]) % self.r) == 0,"r should be a multiple of\
                                                   the length of the signature"
        b = len(signatures[0]) // self.r
        bands = {i: {} for i in range(b)}

        for (idx, s) in enumerate(signatures):
            for i in range(b):
                bucket_id = mmh3.hash128(str(s[i * self.r:(i+1)*self.r]))
                bands[i][bucket_id] = bands[i].get(bucket_id, [])
                bands[i][bucket_id].append(idx)

        res = set()

        # compute combinations for each band
        for i in range(b):
            for bucket_id in bands[i]:
                bucket = bands[i][bucket_id]
                for j in range(len(bucket)):
                    for k in range(j + 1, len(bucket)):
                        res.add(tuple( sorted( (bucket[j], bucket[k]) )))

        if similarity_threshold > 0:
            true_similar = set()
            for (id1, id2) in res:
                same_sigs = sum([1
                                 for i in range(self.r*b)
                                 if signatures[id1][i] == signatures[id2][i]])
                score = same_sigs / (self.r * b)
                if  score >= similarity_threshold:
                    true_similar.add( SimilarPair(id1, id2, score) )
            return true_similar

        return res

class JaccardSimilarity:
    def __init__(self, threshold = 0.8):
        self.threshold = threshold

    def similarPairs(self, shingled_docs):
        """Given a list of docs where each doc is a list of shingles
        produced by `ShingleVectorizer:transform` or 
        `ShingleVectorizer:transformHashed` returns the pairs of indices
        of the docs having similarity higher then the `threshold`.
        The similarity is the Jaccard coefficient between the
        
        Arguments:
            shingled_docs {list(set())}
        
        Returns:
            SimilarPair -- SimilarPair(idx1, idx2, score) namedtuple
        """

        res = set()
        for i in range(len(shingled_docs)):
            for j in range(i + 1, len(shingled_docs)):
                similarity = len(shingled_docs[i] & shingled_docs[j])\
                             / len(shingled_docs[i] | shingled_docs[j])
                if similarity >= self.threshold:
                    res.add(SimilarPair(i, j, similarity))
        return res


if __name__ == '__main__':
    args = parser.parse_args()

    R = args.r
    B = args.b
    K = args.k
    DESCRIPTION_INDEX = args.i
    JACC_THRESHOLD = args.t

    cache = args.file.split('/')[-1] + '_jacc_similarities_K-' +\
            str(K) + '_THRESH-' + str(JACC_THRESHOLD) + '.pickle'

    docs = []
    with open(args.file) as f:
        lines = f.readlines()
        docs = [line.strip().split('\t')[DESCRIPTION_INDEX]
                for line in lines]

    vec = ShinglesVectorizer(k = K)

    shingledDocs = vec.transform(docs)
    hashedShingledDocs = vec.transformHashed(docs)

    lsh_start = time.time()

    l = LocalitySensitiveHashing(r = R)
    h = MinwiseHasher(rb=R * B)
    minHashedDocs = h.transform(hashedShingledDocs)
    resLSH = l.transform(
        minHashedDocs,
        similarity_threshold = JACC_THRESHOLD if args.f else 0
    )

    print('LSH: {} s passed'.format(time.time() - lsh_start))
    print('LSH: {} duplicates found'.format(len(resLSH)))

    jac_start = 0
    jac_end = jac_start

    resJAC = None
    if not os.path.isfile(cache) and not args.skip_jaccard:
        print('Computing Jaccard Similarity... (This might take a while)')
        j = JaccardSimilarity(threshold = JACC_THRESHOLD)

        jac_start = time.time()

        resJAC = j.similarPairs(shingledDocs)

        jac_end = time.time()
        with open(cache, 'wb') as f:
            print('Caching Jaccard Similarity to: ', cache)
            pickle.dump(resJAC, f)
    elif os.path.isfile(cache):
        print('Jaccard Similarity cache found, loading...')
        with open(cache, 'rb') as f:
            resJAC = pickle.load(f)

    if resJAC:
        resLSHwithoutScore = { (e[0], e[1]) for e in resLSH }
        resJACwithoutScore = { (e[0], e[1]) for e in resJAC }


        print('JAC: {} s passed'.format(jac_end - jac_start))
        print('JAC: {} duplicates found'.format(len(resJACwithoutScore)))

        intersect = resJACwithoutScore & resLSHwithoutScore
        print("intersection", len(intersect))
        print("recall", len(intersect) 
                        / len(resJACwithoutScore))
        print("precision", len(intersect)
                            / len(resLSH))
