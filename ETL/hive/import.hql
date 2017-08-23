ADD JAR ${OSM2HIVE_JAR_PATH};
CREATE TEMPORARY FUNCTION OSMImportNodes AS 'info.pavie.osm2hive.controller.HiveNodeImporter';
CREATE TEMPORARY FUNCTION OSMImportWays AS 'info.pavie.osm2hive.controller.HiveWayImporter';
CREATE TEMPORARY FUNCTION OSMImportRelations AS 'info.pavie.osm2hive.controller.HiveRelationImporter';
DROP TABLE IF EXISTS osmdata;
DROP TABLE IF EXISTS osmnodes;
DROP TABLE IF EXISTS osmways;
DROP TABLE IF EXISTS osmrelations;
CREATE TABLE osmdata(osm_content STRING) STORED AS TEXTFILE;
LOAD DATA INPATH '${DATA_PATH}' OVERWRITE INTO TABLE osmdata;
CREATE TABLE osmnodes AS SELECT OSMImportNodes(osm_content) FROM osmdata;
CREATE TABLE osmways AS SELECT OSMImportWays(osm_content) FROM osmdata;
CREATE TABLE osmrelations AS SELECT OSMImportRelations(osm_content) FROM osmdata;


