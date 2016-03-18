--
-- part2 problem 4
-- Create a Pig program that reports the number of URLs served each day in 2012 by the forensicswiki.org website

-- Clear the output directory location
--
-- rmf forensicswiki_count_by_date
rmf forensicswiki_page_top20

--
-- Map locally defined functions to the Java functions in the piggybank
--
DEFINE EXTRACT       org.apache.pig.piggybank.evaluation.string.EXTRACT();

-- This URL uses just one day
-- raw_logs = load 's3://gu-anly502/ps03/forensicswiki.2012-01.unzipped/access.log.2012-01-01' as (line:chararray);
--
-- This URL reads a month:
-- raw_logs = load 's3://gu-anly502/ps03/forensicswiki.2012-01.unzipped/access.log.2012-01-??' as (line:chararray);
--
-- This URL reads all of 2012:
raw_logs = load 's3://gu-anly502/ps03/forensicswiki.2012.txt' as (line:chararray);

 
-- logs_base processes each of the lines 
-- FLATTEN takes the extracted values and flattens them into a single tupple
--
logs_base = 
  FOREACH
   raw_logs
  GENERATE
   FLATTEN ( EXTRACT( line,
     '^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] "(\\S+) (\\S+) \\S+" (\\S+) (\\S+) "([^"]*)" "([^"]*)"'
     ) ) AS (
     host: chararray, identity: chararray, user: chararray, datetime_str: chararray, verb: chararray, url: chararray, request: chararray, status: int,
     size: int, referrer: chararray, agent: chararray
     );

-- YOUR CODE GOES HERE
-- PUT YOUR RESULTS IN output
logs  = FOREACH logs_base GENERATE ToDate(SUBSTRING(datetime_str,0,11),'dd/MMM/yyyy') AS date, host, url, size;
logs2 = FOREACH logs GENERATE SUBSTRING(ToString(date),0,10) AS date, host, url, size;
logs3 = FOREACH logs2 GENERATE REGEX_EXTRACT(date, '^2012.*', 1) AS date, host, url, size;
-- logs = FOREACH logs_base GENERATE group AS url, 
logs4 = FOREACH logs3 GENERATE REGEX_EXTRACT_ALL(url, '(index.php\\?title=|/wiki/)([^ &]*)') AS date, host, url, size;
-- logs4 = FOREACH logs3 GENERATE REGEX_EXTRACT_ALL(ToString(date), '^2012.*') AS date, host, url, size;

by_date = GROUP logs4 BY (url);
-- by_date = FILTER by_date BY ($0 matches '^2012.*');
date_counts = FOREACH by_date GENERATE group AS url, COUNT(logs4);  
-- date_counts = FILTER date_counts BY ($1 matches '2012.*');    

date_counts_sorted = ORDER date_counts BY $1 DESC;

sorted_date20 = limit date_counts_sorted 20;

store sorted_date20 INTO 'forensicswiki_page_top20' USING PigStorage();


-- Get the results
--
fs -getmerge forensicswiki_page_top20 forensicswiki_page_top20.txt

