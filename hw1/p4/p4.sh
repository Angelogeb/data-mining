#! /bin/env bash

awk -F '\t' '{arr[$1]++} END{for (a in arr) print arr[a], a}' ../data/beers.txt | sort -n -r | head -n 10 | cut -d' ' -f2-