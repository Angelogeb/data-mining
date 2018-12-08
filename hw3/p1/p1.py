#!/usr/bin/env python3

import numpy as np

np.seterr(divide="ignore", invalid="ignore")

from collections import Counter

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import k_means, AgglomerativeClustering
from sklearn.metrics import davies_bouldin_score
from sklearn.preprocessing import normalize

from multiprocessing import Pool


def _kmeans(seed):
    def clos(X, k):
        return k_means(X, n_clusters=k, random_state=seed, n_jobs=1)

    return clos


agg = AgglomerativeClustering(
    affinity="cosine",
    linkage="average",
    memory=".",
    compute_full_tree=True,
)


seed = 42
kmeans = _kmeans(seed)

tfidf_vec = TfidfVectorizer(lowercase=False)

# Since the data is normalized by the vectorizer applying the k-means algorithm
# with euclidean distance is the same as using cosine-similarity

X = None
with open("../data/preprocessed_announcements.tsv") as f:
    X = tfidf_vec.fit_transform(f.readlines())

dense_X = X.toarray()


def score_of(k):

    centroid, label, J = kmeans(X, k)
    score = davies_bouldin_score(dense_X, label)

    truncSVD = TruncatedSVD(n_components=k, random_state=seed)
    X_proj_k = normalize(truncSVD.fit_transform(X))

    _, proj_label, J_proj = kmeans(X_proj_k, k)
    proj_score = davies_bouldin_score(dense_X, proj_label)

    print(
        "k: {}, davies X: {}, davies X_proj: {}, J X: {}".format(
            k, score, proj_score, J
        )
    )


for k in range(2, 100, 3):
    agg.set_params(n_clusters=k)
    label = agg.fit_predict(dense_X)
    score = davies_bouldin_score(dense_X, label)
    print(Counter(label))
