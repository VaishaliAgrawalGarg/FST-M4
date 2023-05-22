-- LOAD data from HDFS
inputDialougesIV = LOAD 'hdfs:///user/vaishali/inputs/episodeIV_dialouges.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
inputDialougesV = LOAD 'hdfs:///user/vaishali/inputs/episodeV_dialouges.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
inputDialougesVI = LOAD 'hdfs:///user/vaishali/inputs/episodeVI_dialouges.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
-- Filter out the first 2 lines
rankedIV = RANK inputDialougesIV;
OnlyDialoguesIV = Filter rankedIV BY (rank_inputDialougesIV >2);

rankedV = RANK inputDialougesV;
OnlyDialoguesV = Filter rankedV BY (rank_inputDialougesV >2);

rankedVI = RANK inputDialougesVI;
OnlyDialoguesVI = Filter rankedVI BY (rank_inputDialougesVI >2);

FUllInput = UNION OnlyDialoguesIV, OnlyDialoguesV, OnlyDialoguesVI;

--Group By name
groupByName = GROUP FUllInput BY name;

-- Count the number of lines by each character
names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
namesOdered = ORDER names BY no_of_lines DESC;

--Remove the outputs folder
rmf hdfs:///user/vaishali/outputs;

--Store result in HDFS
STORE namesOdered INTO 'hdfs:///user/vaishali/outputs' USING PigStorage('\t');
