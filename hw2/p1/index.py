#!/usr/bin/env python3
import re
import pickle

from collections import Counter
from math import log10, sqrt

def remove_special_chars(s):
    return re.sub(re.compile(r'\W|\b[a-zA-Z]{,2}\b'), ' ', s).lower()


def preprocess_docs(src_file, dst_file, clean_attrs = {'title', 'description', 'city'}):
    fin = open(src_file)
    fout = open(dst_file, 'w')

    header = fin.readline()
    fout.write(header)
    attrs = header.strip().split('\t')

    for line in fin:
        attr_values = line.strip().split('\t')
        attr_values = [remove_special_chars(v)
                        if attrs[i] in clean_attrs else v
                        for (i, v) in enumerate(attr_values)]

        fout.write('\t'.join(attr_values) + '\n')

    fin.close()
    fout.close()

def doc_frequency(src_file):
    fin = open(src_file)

    header = fin.readline()
    attrs = header.strip().split('\t')

    attr_i = { attr: i for (i, attr) in enumerate(attrs)}

    df = Counter()

    n_docs = 0

    for line in fin:
        n_docs += 1
        attr_values = line.strip().split('\t')
        tokens = set(attr_values[attr_i['description']].split())
        tokens |= set(attr_values[attr_i['title']].split())
        tokens |= set(attr_values[attr_i['city']].split())
        df.update(tokens)
    
    fin.close()

    return (n_docs, df)


def build_index(src_file, dst_file, readable = False):
    fin = open(src_file)
    header = fin.readline()

    mode = 'w' if readable else 'wb'
    fname = dst_file
    fname += '.tsv' if readable else '.pickle'

    fout = open(fname, mode)

    attrs = header.strip().split('\t')
    attr_i = { attr: i for (i, attr) in enumerate(attrs)}

    index = {}

    (n_docs, df) = doc_frequency(src_file)

    for (docId, doc) in enumerate(fin):
        attr_values = doc.strip().split('\t')
        tokens = attr_values[attr_i['description']].split()
        tokens += (attr_values[attr_i['title']].split() * 5) # weights
        tokens += (attr_values[attr_i['city']].split() * 4)  #

        tf = Counter(tokens)
        num = {} # given a term: tf * idf^2
        doc_2norm = 0
        for term in tf:
            num[term] = tf[term] * log10(n_docs/df[term])
            doc_2norm += num[term] ** 2
            num[term] *= log10(n_docs/df[term])

        for term in tf:
            score = num[term] / doc_2norm 
            posting_list = index.get(term, [])
            posting_list.append((docId, score))
            index[term] = posting_list

    if readable:
        for k in sorted(index):
            fout.write(k + '\t' + str(index[k]) + '\n')
    else:
        pickle.dump(index, fout)

    fin.close()
    fout.close()


def load_index(name):
    res = {}
    with open(name + '.pickle', 'rb') as f:
        res = pickle.load()
    return res


if __name__ == '__main__':
    preprocess_docs('file.tsv', 'clean_file.tsv')
    build_index('clean_file.tsv', 'inverted_index', True)