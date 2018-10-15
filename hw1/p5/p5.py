#!/bin/env python3
import heapq

beers = {}

# Create dictionary where key = <beer name>, value = <ratings list>
with open('../data/beers.txt') as f:
    for line in f:
        (beer, rate) = line.split('\t')
        beer = beer.strip()
        rate = int(rate.strip())
        beers[beer] = beers.get(beer, [])
        beers[beer].append(rate)

# [(<avg_rating>, <beer_name>), ... ]
beer_avg = [ (sum(beers[beer]) / len(beers[beer]), beer)
            for beer in beers if len(beers[beer]) >= 100 ]

top_10 = heapq.nlargest(10, beer_avg)

for (_, name) in top_10:
    print(name)
