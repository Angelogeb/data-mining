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

    for (docId, doc) in enumerate(fin, start = 1):
        attr_values = doc.strip().split('\t')
        tokens = attr_values[attr_i['description']].split()
        tokens += (attr_values[attr_i['title']].split()) # weights
        tokens += (attr_values[attr_i['city']].split())  #

        tf = Counter(tokens)
        num = {} # given a term: tf * idf^2
        doc_2norm = 0
        for term in tf:
            num[term] = tf[term] * log10(n_docs/df[term])
            doc_2norm += num[term] ** 2
            num[term] *= log10(n_docs/df[term])

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
    q_tf = Counter(remove_special_chars(q).split())
    heap = [HeapEntry(0,0)]
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

        current_docIds = [index[t][pointers[t]].docId for t in pointers if pointers[t] < len(index[t])]
        min_docId = min(current_docIds) if current_docIds else -1

    return sorted(heap, reverse = True)

        


if __name__ == '__main__':
    doc = """Linux System Administrator:  Requisiti ricercati: - Esperienza nel ruolo di Sistemista di almeno 2-3 anni; - Competenze tecniche Sistemi operativi Linux; - Conoscenza di base di integrazione di sistemi informatici; - Conoscenza di linguaggi di scripting, in particolare shell.  Costituiscono titolo preferenziale: - Conoscenza dello stack ELK e/o di altre soluzioni di log management; - Conoscenza di soluzioni di network e data security.  Attività proposta:  La risorsa verrà inserita in un team che gestisce le seguenti attività: - Amministrazione di sistemi Linux; - Troubleshooting sui sistemi Linux; - System Maintenance su hardware e software; - Analisi e patching dei sistemi Linux; - Tuning per le performance e assicurare l'alta affidabilità dell'infrastruttura; - Implementazione ed integrazione soluzioni di log management.  L'opportunità prevede un inserimento con contratto a tempo indeterminato
    """
    preprocess_docs('file.tsv', 'clean_file.tsv')
    index = build_index('clean_file.tsv', 'inverted_index', True)
    res = process_query(doc, index, 3)
    for (score, docId) in res:
        print("# " + str(docId))
        print("score: " + str(score))
        print(linecache.getline("file.tsv", docId + 1))