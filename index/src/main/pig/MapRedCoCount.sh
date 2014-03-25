#!/bin/sh
hadoop fs -copyFromLocal ../../../output/occs.tsv .
pig -param inFile=occs.tsv -param outDir=. CooccurrencesCount.pig
hadoop fs -get co-occs-count.json ../../../output/co-occs-count.json
