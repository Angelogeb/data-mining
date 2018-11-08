#!/usr/bin/env python3
import re
import pickle
import heapq
import linecache

from collections import Counter, namedtuple
from math import log10, sqrt

PostingListEntry = namedtuple('PostingListEntry', ['docId', 'partialScore'])
HeapEntry = namedtuple('HeapEntry', ['score', 'docId'])

def remove_special_chars(s):
    return re.sub(re.compile(r'\W|\b[a-zA-Z]{,2}\b'), ' ', s).lower()

def preprocess_docs(src_file,
                    dst_file,
                    clean_attrs = {'title', 'description', 'city'}):
    fin = open(src_file)
    fout = open(dst_file, 'w')

    header = fin.readline()

    attrs = header.strip().split('\t')

    for line in fin:
        attr_values = line.strip().split('\t')
        res = []
        for (i, v) in enumerate(attr_values):
            if attrs[i] in clean_attrs:
                s = remove_special_chars(v)
                s = " ".join(s.split())
                res.append(s)

        fout.write('\t'.join(res) + '\n')

    fin.close()
    fout.close()
    return [ attr for attr in attrs if attr in clean_attrs ]

def doc_frequency(src_file):

    fin = open(src_file)


    df_term = Counter()
    n_docs = 0

    for line in fin:
        n_docs += 1
        terms = set(line.strip().split())
        df_term.update(terms)

    fin.close()

    return (n_docs, df_term)


def build_index(src_file, dst_file, attrs, readable = False):
    fin = open(src_file)

    mode = 'w' if readable else 'wb'
    fname = dst_file
    fname += '.tsv' if readable else '.pickle'

    fout = open(fname, mode)

    attr_i = { attr: i for (i, attr) in enumerate(attrs)}

    index = {}

    (n_docs, df) = doc_frequency(src_file)

    for (docId, doc) in enumerate(fin, start = 1):
        attr_values = doc.strip().split('\t')

        tokens = []
        for attr in attrs:
            tokens += attr_values[attr_i[attr]].split()

        tf = Counter(tokens)
        num = {} # given a term: tf * idf^2
        doc_2norm = 0
        for term in tf:
            idf = log10(n_docs/df[term])
            num[term] = tf[term] * idf
            doc_2norm += num[term] ** 2
            num[term] *= idf

        for term in tf:
            partialScore = num[term] / sqrt(doc_2norm) 
            posting_list = index.get(term, [])
            posting_list.append(PostingListEntry(docId, partialScore))
            index[term] = posting_list

    if readable:
        for k in sorted(index):
            fout.write(k + '\t' + str(index[k]) + '\n')
    else:
        pickle.dump(index, fout)

    fin.close()
    fout.close()
    return index

def load_index(name):
    with open(name + '.pickle', 'rb') as f:
        return pickle.load(f)

def min_heap(heap):
    return heap[0]

def process_query(q, index, k):
    """Term at a time query processing
    
    Arguments:
        q {string} -- Query
        index {dict} -- Index dictionary containing posting lists
        k {int} -- Maximum number of results to be retrieved
    
    Returns:
        list -- List of tuples (score, docId) sorted by score
    """

    q_tf = Counter(remove_special_chars(q).split())
    heap = []
    pointers = {t: 0 for t in q_tf if t in index }

    current_docIds = [index[t][pointers[t]].docId for t in pointers]
    min_docId = min(current_docIds) if current_docIds else -1

    while min_docId != -1:
        score = 0

        for t in pointers:
            if pointers[t] < len(index[t]):
                elem = index[t][pointers[t]]
                if elem.docId == min_docId:
                    score += elem.partialScore * q_tf[t]
                    pointers[t] += 1

        if len(heap) < k:
            heapq.heappush(heap, HeapEntry(score, min_docId))
        elif score > min_heap(heap).score:
            heapq.heappushpop(heap, HeapEntry(score, min_docId))

        current_docIds = [index[t][pointers[t]].docId
                          for t in pointers
                          if pointers[t] < len(index[t])]
        min_docId = min(current_docIds) if current_docIds else -1

    return sorted(heap, reverse = True)


RAW_TSV_FILE = '../data/retrieved_announcements.tsv'
PREPROCESSED_FILE = '../data/preprocessed_announcements.tsv'
INDEX_FILE = 'inverted_index'

if __name__ == '__main__':
    doc = 'Linux System Administrator'
    attrs = preprocess_docs(RAW_TSV_FILE, PREPROCESSED_FILE)
    index = build_index(PREPROCESSED_FILE, INDEX_FILE, attrs)
    res = process_query(doc, index, 10)
    for (score, docId) in res:
        print("# " + str(docId))
        print("score: " + str(score))
        print(linecache.getline(RAW_TSV_FILE, docId + 1))
