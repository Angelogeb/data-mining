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
    static_folder='frontend/dist'
)
app.config['ENV'] = 'development'

INDEX_URL = "http://localhost:5000/index.html"

RAW_TSV_FILE = '../data/retrieved_announcements.tsv'
PREPROCESSED_FILE = '../data/preprocessed_announcements.tsv'
INDEX_FILE = 'inverted_index'

attrs = preprocess_docs(RAW_TSV_FILE, PREPROCESSED_FILE)
index = build_index(PREPROCESSED_FILE, INDEX_FILE, attrs, False)

header = ['title', 'href', 'city', 'timestamp', 'description']

@app.route('/search', methods=['POST'])
def search():
    content = request.get_json(silent = True)
    query = content['query']
    start = time.time()
    docs = process_query(query, index, 100)
    
    result = {}
    result['documents'] = []
    for score, docId in docs:
        line = linecache.getline(RAW_TSV_FILE, docId + 1).strip().split('\t')
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