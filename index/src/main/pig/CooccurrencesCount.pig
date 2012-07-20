-- This script is to count the co-occurrences from entities from the TSV file generated by ExtractOccsFromWikipedia.
-- The output would be stored into JSON format.
-- @param inFile  the path to where the occs.tsv stores (generated by ExtractOccsFromWikipedia)
-- @param outDir the output dir
-- @param useDocLevel  A flag to decide whether to count co-occurrences based on paragraphs or documents, true to use document level, false to use paragraph level, default is false
-- @param minCount The minimum count that a co-occurrence  will be reserved. Default is 3
-- @author hector.liu

SET job.name CooccurrencesCount;

--register udf
Register 'index_pig_udf.py' using jython as funcs;

%default inFile occs.tsv
%default outDir . -- do not include slash at the end 
%default useDocLevel false
%default minCount 3

-- Go through all the occurrences in the TSV file
occs = LOAD '$inFile' USING PigStorage('\t') AS (id:chararray,uri:chararray,surfaceForm:chararray,context:chararray,offset:chararray);

-- Get doc id and entity id
-- URI is used as entity id
docEntPairs = FOREACH occs GENERATE  (($useDocLevel == false) ? funcs.getDocParaId(id) : funcs.getDocId(id) ) , uri;

-- Clean up for empty doc id
cleanedDocEntPairs = FILTER docEntPairs BY (reducedId != '') AND (reducedId is not null);

-- Group by reduced id, it could be a doc id, or a doc id + paragraph number
coOccsById = GROUP cleanedDocEntPairs BY reducedId;

-- Flatten by itself to produce cross product for the list of uri
entityPairs = FOREACH coOccsById {
	uri = cleanedDocEntPairs.uri;
	sortedUri = order uri by uri;
	generate flatten(sortedUri) AS (src) ,flatten(sortedUri) AS (tar);
};

-- Self co-occurrences are not desired
cleanedPairs = FILTER entityPairs BY src!= tar;

--Group by entity co-occured pair
groupedPairs = GROUP cleanedPairs BY (src,tar);

-- Count the entity pairs
cnt = FOREACH groupedPairs GENERATE group.src,group.tar, COUNT(cleanedPairs) AS count;

-- Cooccurrences less than $minCount will be removed
reducedCnt = FILTER cnt BY count>=$minCount;

-- Group into a ajancency list
adjLists = GROUP reducedCnt BY src;

-- Format to a JSON like format 
JSONAdjLists = FOREACH adjLists GENERATE group AS src,reducedCnt.(tar,count) AS counts;

describe JSONAdjLists;

-- Write out
-- Consider use JSONStorage
STORE JSONAdjLists INTO '$outDir/co-occs-count.json' USING JsonStorage();