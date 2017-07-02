#!/usr/bin/env python
from __future__ import print_function
import commands
from time import time
import sys

def main(argv):
    t0 = time()
    print()
    
    print("Importing OSM data to Hive.")
    

    cmd = []
    cmd.append("hive -e 'DROP TABLE IF EXISTS osmdata;'")
    cmd.append("hive -e 'CREATE TABLE osmdata(osm_content STRING) STORED AS TEXTFILE;'")
    cmd.append("hive -e 'LOAD DATA LOCAL INPATH \"/home/cloudera/Documents/opendata/map.xml\" OVERWRITE INTO TABLE osmdata;'")
    cmd.append("hive -e 'DROP TABLE IF EXISTS osmnodes;'")
    cmd.append("hive -e 'DROP TABLE IF EXISTS osmways;'")
    cmd.append("hive -e 'DROP TABLE IF EXISTS osmrelations;'")

    for c in cmd:
        print("Executing: " + c)
        status, output = commands.getstatusoutput(c)
        
        if status == 0:
            print(output)
        else:
            print(output)

    cmd = "hive -e 'ADD JAR /home/cloudera/jars/OSM2Hive.jar; " + \
                   "CREATE TEMPORARY FUNCTION OSMImportNodes AS \"info.pavie.osm2hive.controller.HiveNodeImporter\"; " + \
                   "CREATE TABLE osmnodes AS SELECT OSMImportNodes(osm_content) FROM osmdata;'"

    print("Executing: " + cmd)
    status, output = commands.getstatusoutput(cmd)
        
    if status == 0:
        print(output)
    else:
        print(output)

    cmd = "hive -e 'ADD JAR /home/cloudera/jars/OSM2Hive.jar; " + \
          "CREATE TEMPORARY FUNCTION OSMImportWays AS \"info.pavie.osm2hive.controller.HiveWayImporter\";" + \
          "CREATE TABLE osmways AS SELECT OSMImportWays(osm_content) FROM osmdata;'"

    print("Executing: " + cmd)
    status, output = commands.getstatusoutput(cmd)
        
    if status == 0:
        print(output)
    else:
        print(output)


    cmd = "hive -e 'ADD JAR /home/cloudera/jars/OSM2Hive.jar; " + \
          "CREATE TEMPORARY FUNCTION OSMImportRelations AS \"info.pavie.osm2hive.controller.HiveRelationImporter\";" + \
          "CREATE TABLE osmrelations AS SELECT OSMImportRelations(osm_content) FROM osmdata;'"

    print("Executing: " + cmd)
    status, output = commands.getstatusoutput(cmd)
        
    if status == 0:
        print(output)
    else:
        print(output)

    elapsed = time() - t0
    print("Total elapsed time: " + str(format(round(elapsed,3))))

       
if __name__ == "__main__":
    main(sys.argv[1:])
