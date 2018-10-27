#!/usr/bin/env python3

import itertools

try:
    import mmh3 as mmh3
except ImportError:
    import pymmh3 as mmh3

def murmurHash(seed):
    def resFun(s):
        return mmh3.hash128(s, seed)
    return resFun

class ShinglesVectorizer:
    def __init__(self, k = 10):
        self.k = k
    
    def transform(self, documents):
        if type(documents) != list: documents = [documents]
        assert (len(documents[0]) - self.k + 1 > 0), "k should be less than the\
                                                      document length"
        return [set([d[i:i+self.k] for i in range(len(d) - self.k + 1)])
                for d in documents]
    
    def transformHashed(self, documents, hash_fun = mmh3.hash128):
        if type(documents) != list: documents = [documents]

        assert (len(documents[0]) - self.k + 1 > 0), "k should be less than the\
                                                      document length"
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

                if  same_sigs / self.r*b > similarity_threshold:
                    true_similar.add( (id1, id2) )
            return true_similar

        return res

if __name__ == '__main__':
    docs = ["today there is a strike", "today there is a strike for real"]
    vec = ShinglesVectorizer(k = 2)
    h = MinwiseHasher(rb=10)
    hashedShingledDocs = vec.transformHashed(docs)
    minHashedDocs = h.transform(hashedShingledDocs)
    l = LocalitySensitiveHashing(r = 5)
    print(l.transform(minHashedDocs))