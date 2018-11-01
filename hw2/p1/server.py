#!/usr/bin/env python3

import webbrowser
import threading
import requests
import linecache
import time

from flask import Flask, jsonify, request
from index import build_index, doc_frequency, process_query, preprocess_docs

app = Flask(
    __name__,
    static_url_path='',
    static_folder='dist'
)
app.config['ENV'] = 'development'

INDEX_URL = "http://localhost:5000/index.html"

attrs = preprocess_docs('file.tsv', 'clean_file.tsv')
index = build_index('clean_file.tsv', 'inverted_index', attrs, True)

header = ['title', 'href', 'city', 'timestamp', 'description']

@app.route('/search', methods=['POST'])
def search():
    content = request.get_json(silent = True)
    query = content['query']
    start = time.time()
    docs = process_query(query, index, 10)
    
    result = {}
    result['documents'] = []
    for score, docId in docs:
        line = linecache.getline('file.tsv', docId + 1).strip().split('\t')
        curr_doc = {}
        for fid, field in enumerate(header):
            curr_doc[field] = line[fid]
        curr_doc['score'] = score
        curr_doc['id'] = docId + 1
        result['documents'].append(curr_doc)
    
    process_time = time.time() - start
    result['time'] = process_time

    return jsonify(result)

def open_browser():
    while True:
        try:
            requests.get(INDEX_URL)
            break
        except Exception:
            time.sleep(1)
    webbrowser.open_new_tab(INDEX_URL)

threading.Thread(target=open_browser).start()

app.run()