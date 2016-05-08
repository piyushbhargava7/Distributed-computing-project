
SELECT "Current Node", NODE_1 FROM CURRENT1;

INSERT OVERWRITE TABLE open
SELECT * FROM open a
WHERE a.node_1 not in (select node_1 from current1);


INSERT INTO TABLE closed
SELECT * FROM current1;


INSERT OVERWRITE TABLE temp_scores
SELECT original.node_2 as node, a.g_score + original.dist/10000 as g_score, 
a.f_score as f_score FROM (select * from current1) a left join
original
on a.node_1=original.node_1
where original.node_2 not in (select node_1 from closed);


insert into TABLE came_from
select o.node_2 as node, current1.node_1 as node_from from current1 inner join
original o on current1.node_1=o.node_1 
where o.node_2 not in (select node_1 from closed union all select node_1 from temp_scores inner join open on temp_scores.node=open.node_1 where temp_scores.g_score>=open.g_score);


INSERT INTO TABLE open
select distinct a.node_2, d.g_score as g_score, 
d.g_score + 6372.8*2*asin(sqrt(POW(sin(radians(lat2 - lat1)/2),2) + POW(cos(radians(lat1))*cos(radians(lat2))*sin(radians(lon2 - lon1)/2),2))) as f_score 
from (select node_1, node_2 from original i where i.node_1 IN (select node_1 from current1)) a 
left join 
(select node as node_1, latitude/1000000 as lat1, longitude/1000000 as lon1 from coordinates) b
on a.node_2 = b.node_1
left join 
(select node as node_2, latitude/1000000 as lat2, longitude/1000000 as lon2 from coordinates) c
left join
(select node, g_score from temp_scores) d
on a.node_2 = d.node
where c.node_2 = ${hiveconf:stop} and a.node_2 not in (select node_1 from closed)
;


INSERT OVERWRITE TABLE open
select node_1, g_score, f_score from (select node_1, g_score, f_score, row_number() OVER (PARTITION BY node_1 ORDER BY g_score) as rn
from open) a where a.rn=1;


INSERT OVERWRITE TABLE current1
SELECT * from open order by f_score limit 1;


