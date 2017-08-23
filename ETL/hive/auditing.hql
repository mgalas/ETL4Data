DELETE FROM errors;
DELETE FROM stats;

--//////////////////
--001_deleted_items

--deleted nodes in ways
INSERT INTO ERRORS
SELECT '010', 'Nodes', w.id, 'Deleted nodes still included in ways', current_timestamp()
FROM 
(SELECT id,isvisible from osmnodes) n
JOIN
(SELECT id, node, isvisible FROM osmways 
LATERAL VIEW explode(nodes) nodeTable as node) w
ON (n.id = w.node)
WHERE (n.isvisible = FALSE AND w.isvisible = TRUE);

--deleted nodes in relations
INSERT INTO ERRORS
SELECT '011', 'Nodes', r.id, 'Deleted nodes still included in relations', current_timestamp()
FROM 
(SELECT id,isvisible from osmnodes) n
JOIN
(SELECT id, isvisible, member,type FROM osmrelations 
LATERAL VIEW explode(members) dummy_table as member, type
WHERE isvisible = TRUE) r
ON (n.id = r.member)
WHERE (n.isvisible = FALSE);

--deleted ways in relations
INSERT INTO ERRORS
SELECT '012', 'Ways', r.id, 'Deleted ways still included in relations', current_timestamp()
FROM 
(SELECT id,isvisible from osmways) w
JOIN
(SELECT id, isvisible, member,type FROM osmrelations 
LATERAL VIEW explode(members) dummy_table as member, type
WHERE isvisible = TRUE) r
ON (w.id = r.member)
WHERE (w.isvisible = FALSE);

--//////////////////////////////////////////
--0100 places of worhip without religion tag

--nodes
INSERT INTO ERRORS
SELECT '100', 'Nodes', b.id, 'Places of worhip without religion tag', current_timestamp()
FROM (
	SELECT id,key,value
	FROM osmnodes
	LATERAL VIEW explode(tags) dummy_table as key,value
	where key = 'amenity' AND value ='place_of_worship') b
WHERE 1 = 1 AND NOT EXISTS (
	SELECT * 
	FROM (
		SELECT id, key,value
		FROM osmnodes
		LATERAL VIEW explode(tags) dummy_table as key,value
		WHERE key in ('religion', 'denomination')) c 
	WHERE c.id = b.id);

--ways
INSERT INTO ERRORS
SELECT '100', 'Ways', b.id, 'Places of worhip without religion tag', current_timestamp()
FROM (
	SELECT id,key,value
	FROM osmways
	LATERAL VIEW explode(tags) dummy_table as key,value
	where key = 'amenity' AND value ='place_of_worship') b
WHERE 1 = 1 AND NOT EXISTS (
	SELECT * 
	FROM (
		SELECT id, key,value
		FROM osmways
		LATERAL VIEW explode(tags) dummy_table as key,value
		WHERE key in ('religion', 'denomination')) c
	WHERE c.id = b.id);

--////////////////
--0120
INSERT INTO ERRORS
SELECT '120', 'Ways', id, 'Ways without nodes', current_timestamp()
FROM osmways
where size(nodes) = 0;

INSERT INTO ERRORS
SELECT '120', 'Ways', id, 'Ways without nodes', current_timestamp()
FROM osmways
where nodes is NULL;

--///////////////////
--0140

INSERT INTO ERRORS
SELECT '140', 'Ways', a.id, 'Ways without highway tag', current_timestamp()
FROM (
	SELECT id,min(value) as v
	FROM osmways
	LATERAL VIEW explode(tags) dummy_table as key,value
	GROUP BY id) a
WHERE NOT EXISTS (
	SELECT *
	FROM (
		SELECT id,key,value
		FROM osmways
		LATERAL VIEW explode(tags) dummy_table as key,value) b
	WHERE a.id = b.id AND 
	b.key NOT IN ('highway', 'history', 'natural', 'railway', 'building', 'amenity', 'boundary', 'leisure', 'aerialway', 'boat', 'bridge', 'tunnel', 'shop', 'area', 'tourism', 'place', 'man_made', 'wood', 'cycleway')
);

--////////////////
--180
INSERT INTO ERRORS
SELECT '180', 'Relations', r.id, 'Relations without type', current_timestamp()
FROM osmrelations r
WHERE NOT EXISTS (
	SELECT *
	FROM (
		SELECT id,key,value
		FROM osmrelations
		LATERAL VIEW explode(tags) dummy_table as key,value
		where key = 'type'
	) r2
	WHERE r2.id = r.id
);

--////////////////
--0380
INSERT INTO ERRORS
SELECT '380', 'Ways', a.id, 'Sport elements without type description', current_timestamp()
FROM (
	SELECT id,key,value
	FROM osmways
	LATERAL VIEW explode(tags) dummy_table as key,value
	WHERE key = 'sport') a
WHERE NOT EXISTS (
	SELECT *
	FROM (
		SELECT id,key,value
		FROM osmways
		LATERAL VIEW explode(tags) dummy_table as key,value) b
	WHERE a.id = b.id AND (b.key in ('leisure', 'piste', 'building', 'natural', 'landuse', 'highway', 'bridge', 'ski_resort', 'route', 'tourism', 'amenity', 'shop') OR b.key LIKE 'piste:%')
)
GROUP BY a.id;

--///////////////////
--0390
INSERT INTO ERRORS
SELECT '390', 'Ways', a.id, 'Track ways without description', current_timestamp()
FROM (
	SELECT id,key,value
	FROM osmways
	LATERAL VIEW explode(tags) dummy_table as key,value
	WHERE key = 'highway' AND value = 'track') a
WHERE NOT EXISTS (
	SELECT *
	FROM (
		SELECT id,key,value
		FROM osmways
		LATERAL VIEW explode(tags) dummy_table as key,value) b
	WHERE a.id = b.id AND b.key = 'tracktype'
)
GROUP BY a.id;

--///////////////////
--0420
--nodes
INSERT INTO ERRORS
SELECT '420', 'Nodes', id, 'Tags with suspicious values', current_timestamp()
FROM osmnodes
LATERAL VIEW explode(tags) dummy_table as key,value
WHERE key = 'incline' AND value != '0' AND value LIKE '\d' AND value NOT LIKE '^[+-]?\d+(\.\d+)?[\%\°]?$';

--/////////STATS
--NODES
--Total nodes
INSERT INTO STATS
SELECT 'Number of nodes', cast(count(*) as string), current_timestamp()
FROM osmnodes;

--Average tags per node
INSERT INTO STATS
SELECT 'Average number of tags per node', cast(avg(a.node_count) as string), current_timestamp()
FROM (
	SELECT size(tags) as node_count
	FROM osmnodes) a;
--Average version
SELECT avg(version)
FROM osmnodes;

--RELATIONS
--Number of routes
INSERT INTO STATS
SELECT 'Number of routes', cast(count(DISTINCT a.id) as string), current_timestamp()
FROM (
	SELECT id,key,value
	FROM osmrelations
	LATERAL VIEW explode(tags) dummy_table as key,value
	WHERE key = 'type' and value = 'route'
) a;

--Number of multiploygons
INSERT INTO STATS
SELECT 'Number of multipolygon areas', cast(count(DISTINCT a.id) as string), current_timestamp()
FROM (
	SELECT id,key,value
	FROM osmrelations
	LATERAL VIEW explode(tags) dummy_table as key,value
	WHERE key = 'type' and value = 'multipolygon'
) a;

--ALL
--Total users
INSERT INTO STATS
SELECT 'Number of users', cast(count(DISTINCT a.userid) as string), current_timestamp()
FROM (
	SELECT DISTINCT userid
	FROM osmnodes
	UNION ALL
	SELECT DISTINCT userid
	FROM osmways
	UNION ALL
	SELECT DISTINCT userid
	FROM osmrelations) a;



