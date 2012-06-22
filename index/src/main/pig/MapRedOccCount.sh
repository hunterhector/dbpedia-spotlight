hadoop fs -copyFromLocal ../../../output/occs.tsv .
pig OccurrencesCount.pig
hadoop fs -getmerge occurrences-count-ids.tsv ../../../output/occurrences-count-ids.tsv

