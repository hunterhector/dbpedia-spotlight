-- This script is to count the co-occurrences from entities from the TSV file generated by ExtractOccsFromWikipedia.
-- The output would be stored into JSON like format.
-- @param dir  the directory where the occs.tsv stores (generated by ExtractOccsFromWikipedia)
-- @param useDocLevel  A flag to decdie whether to count co-occurrences based on paragraphs or documents, 1 to use document level, 0 to use paragraph level, default is 0
-- @author hector.liu

--register udf
Register 'index_pig_udf.py' using jython as funcs;

%default dir ../../../output/
%default useDocLevel 0

-- Go through all the occurrences in the TSV file
occs = LOAD '$dir/occs.tsv' USING PigStorage('\t') AS (id:chararray,uri:chararray,surfaceForm:chararray,context:chararray,offset:chararray);

-- Get doc id and entity id
-- URI is used as entity id
docEntPairs = FOREACH occs GENERATE  (($useDocLevel == 0) ? funcs.getDocParaId(id) : funcs.getDocId(id) ) , uri;

-- Clean up for empty doc id
cleanedDocEntPairs = FILTER docEntPairs BY (reducedId != '') AND (reducedId is not null);

-- Group by reduced id, it could be a doc id, or a doc id + paragraph number
coOccs = GROUP cleanedDocEntPairs BY reducedId;

-- Generate combination of entity id to get all co-occured pairs using udf
entityPairs = FOREACH coOccs GENERATE FLATTEN(funcs.getCombination(cleanedDocEntPairs.uri));

/* This implementation discarded because GROUP is not supported in nested FOREACH
-- Group by the first entity(ent1) first to ceate an ajancency list like thing
-- adjList = GROUP entityPairs BY ent1;
-- dump adjList;

-- Count the number of repeated ent2, which is effectively number of (ent1, ent2)
-- countedAdjList = foreach adjList{
--	GENERATE ent2, COUNT(ent2)
-- };
-- dump countedAdjList;
*/

--Group by entity co-occured pair
groupedPairs = GROUP entityPairs BY (ent1,ent2);
-- Count the entity pairs
cnt = FOREACH groupedPairs GENERATE group.ent1,group.ent2, COUNT(entityPairs);
-- Group into a ajancency list
adjLists = GROUP cnt BY ent1;
-- Remove duplicate in the list 
reducedAdjLists = FOREACH adjLists GENERATE group,cnt.($1,$2);
-- Format and write out
STORE reducedAdjLists INTO '$dir/co-occs-count.out' USING PigStorage();