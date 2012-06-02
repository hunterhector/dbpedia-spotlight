-- This script is to count the co-occurrences from entities from the TSV file generated by ExtractOccsFromWikipedia.
-- The output would be stored into JSON like format.
-- @param dir  the directory where the occs.tsv stores (generated by ExtractOccsFromWikipedia)
-- @param useDocLevel  A flag to decdie whether to count co-occurrences based on paragraphs or documents, 1 to use document level, 0 to use paragraph level, default is 0
-- @param minCount The minimum count that a co-occurrence  will be reserved. Default is 3
-- @author hector.liu

--register udf
Register 'index_pig_udf.py' using jython as funcs;

%default dir ../../../output/
%default useDocLevel 0
%default minCount 3

-- Go through all the occurrences in the TSV file
occs = LOAD '$dir/occs.tsv.small' USING PigStorage('\t') AS (id:chararray,uri:chararray,surfaceForm:chararray,context:chararray,offset:chararray);

-- Get doc id and entity id
-- URI is used as entity id
docEntPairs = FOREACH occs GENERATE  (($useDocLevel == 0) ? funcs.getDocParaId(id) : funcs.getDocId(id) ) , uri;

-- Clean up for empty doc id
cleanedDocEntPairs = FILTER docEntPairs BY (reducedId != '') AND (reducedId is not null);

-- Group by reduced id, it could be a doc id, or a doc id + paragraph number
coOccsById = GROUP cleanedDocEntPairs BY reducedId;

-- Flatten to cross product the list of uri
entityPairs = FOREACH coOccsById {
	uri = cleanedDocEntPairs.uri;
	sortedUri = order uri by uri;
	generate flatten(sortedUri) AS (ent1) ,flatten(sortedUri) AS (ent2); 
};

-- Self co-occurrences are not desired
cleanedPairs = FILTER entityPairs BY ent1!= ent2;

--Group by entity co-occured pair
groupedPairs = GROUP cleanedPairs BY (ent1,ent2);

-- Count the entity pairs
cnt = FOREACH groupedPairs GENERATE group.ent1,group.ent2, COUNT(cleanedPairs) AS count;

-- Cooccurrences less than $minCount will be removed
reducedCnt = FILTER cnt BY count>=$minCount;

-- Group into a ajancency list
adjLists = GROUP reducedCnt BY ent1;

-- Format to a JSON like format 
JSONAdjLists = FOREACH adjLists GENERATE group,reducedCnt.(ent2,count);

-- Write out
-- Consider use JSONStorage
STORE JSONAdjLists INTO '$dir/co-occs-count.out' USING PigStorage();