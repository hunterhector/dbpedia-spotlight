-- This script is to count the co-occurrences from entities from the TSV file generated by ExtractOccsFromWikipedia.
-- @param dir  the directory where the occs.tsv stores (generated by ExtractOccsFromWikipedia)
-- @author hector.liu

--register udf
Register 'index_pig_udf.py' using jython as funcs;

%declare dir ../../../output/

-- Go through all the occurrences in the TSV file
occs = LOAD '$dir/occs.tsv' USING PigStorage('\t') AS (id,uri,surfaceForm,context,offset);

-- Get doc id and entity id
-- Used STRSPLIT to get the docid which is portion after the '-' sign of the whold id
-- URI is used as entity id
docEntPairs = FOREACH occs GENERATE  STRSPLIT(id,'-',2).$1 , uri;

-- Clean up for empty doc id
cleanedDocEntPairs = FILTER docEntPairs BY ($0 is not null) AND ($0 != '');

-- Group by doc id
coOccs = GROUP cleanedDocEntPairs BY $0;

-- Generate combination of entity id to get all co-occured pairs using udf
entityPairs = FOREACH coOccs GENERATE FLATTEN(funcs.getCombination(cleanedDocEntPairs.uri));

-- Group by entity co-occured pair
groupedPairs = GROUP entityPairs BY (ent1,ent2);

-- Count the entity pairs
cnt = FOREACH groupedPairs GENERATE group, COUNT(entityPairs);

-- Format and write out
STORE cnt INTO '$dir/co-occs-count.tsv' USING PigStorage();
