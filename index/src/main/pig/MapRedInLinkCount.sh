hadoop fs -copyFromLocal ../../../output/occs.tsv .
pig CommonInlinkCount.pig
hadoop fs -getmerge commonInlinkCount.tsv ../../../output/commonInlinkCount.tsv
