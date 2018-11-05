#! /usr/bin/env sh

pandoc -s --mathjax report.txt -o report.html --toc --css pandoc.css