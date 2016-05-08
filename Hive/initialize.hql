--reading data from s3
DROP TABLE IF EXISTS original;
CREATE TABLE original(c1 string, node_1 string, node_2 string, dist INT) 
ROW FORMAT DELIMITED                                                                   
FIELDS TERMINATED BY ' '   
STORED AS TEXTFILE;   

LOAD DATA INPATH '/data/bay_dist.txt'
OVERWRITE INTO TABLE original;


--reading data from s3
DROP TABLE IF EXISTS coordinates;
CREATE TABLE coordinates(c1 string, node string, longitude INT, latitude INT) 
ROW FORMAT DELIMITED                                                                   
FIELDS TERMINATED BY ' '   
STORED AS TEXTFILE;   

LOAD DATA INPATH '/data/bay_co.txt'
OVERWRITE INTO TABLE coordinates;


--initializing tables with start node
DROP TABLE IF EXISTS open;
CREATE TABLE open(node_1 string, g_score float, f_score float) 
STORED AS TEXTFILE; 

INSERT INTO TABLE open
select distinct a.node_1, 0 as g_score, 6372.8*2*asin(sqrt(POW(sin(radians(lat2 - lat1)/2),2) + POW(cos(radians(lat1))*cos(radians(lat2))*sin(radians(lon2 - lon1)/2),2))) as f_score 
from original c 
left join 
(select node as node_1, latitude/1000000 as lat1, longitude/1000000 as lon1 from coordinates) a
on c.node_1 = a.node_1
left join 
(select node as node_2, latitude/1000000 as lat2, longitude/1000000 as lon2 from coordinates) b
where c.node_1 = ${hiveconf:start} AND b.node_2 = ${hiveconf:stop};


DROP TABLE IF EXISTS closed;
CREATE TABLE closed(node_1 string, g_score float, f_score float) 
STORED AS TEXTFILE; 


DROP TABLE IF EXISTS came_from;
CREATE TABLE came_from(node string, node_from string) 
STORED AS TEXTFILE; 


DROP TABLE IF EXISTS temp_scores;
CREATE TABLE temp_scores(node string, g_score float, f_score float) 
STORED AS TEXTFILE; 

INSERT INTO TABLE temp_scores
SELECT node_1 as node, 0 as g_score, f_score from open;

DROP TABLE IF EXISTS current1;
CREATE TABLE current1(node_1 string, g_score float, f_score float) 
STORED AS TEXTFILE; 

INSERT OVERWRITE TABLE current1
SELECT * from open order by f_score limit 1;
