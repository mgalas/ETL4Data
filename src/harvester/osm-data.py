#!/usr/bin/env python3
from __future__ import print_function
import subprocess
from time import time
import sys
import os.path
from clint.textui import progress
import requests
import bz2

def main(argv):

    url = "http://download.geofabrik.de/europe/great-britain/england/greater-london-latest.osm.bz2"
    directory = os.path.join(argv[0])
    download_path = os.path.join(directory, "planet.osm.bz2")
    data_path = os.path.join(directory, "planet.xml")
    jar_path = "/home/ulincero/jars/OSM2Hive.jar"

    if argv[1] == "yes":

        print("Downloading planet file...")

        response = requests.get(url, stream=True)
        with open(download_path, 'wb') as f:
            total_length = int(response.headers.get('content-length'))
            for chunk in progress.bar(response.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1): 
                if chunk:
                    f.write(chunk)
                    f.flush()
        
        print("Decompressing file...")
        
        with open(data_path, 'wb') as new_file, bz2.BZ2File(download_path, 'rb') as file:
            for data in iter(lambda : file.read(100 * 1024), b''):
                new_file.write(data)
    

    print("Importing OSM data to Hive.")
    

    cmd = []
    cmd.append("DROP TABLE IF EXISTS osmdata;")
    #cmd.append("hive -e 'CREATE TABLE osmdata(osm_content STRING) STORED AS TEXTFILE;'")
    #cmd.append("hive -e 'LOAD DATA LOCAL INPATH \"/home/cloudera/Documents/opendata/map.xml\" OVERWRITE INTO TABLE osmdata;'")
    #cmd.append("hive -e 'DROP TABLE IF EXISTS osmnodes;'")
    #cmd.append("hive -e 'DROP TABLE IF EXISTS osmways;'")
    #cmd.append("hive -e 'DROP TABLE IF EXISTS osmrelations;'")

    for c in cmd:
        print("Executing: " + c)

        try:
            output = subprocess.check_output(["hive", "-e", "DROP TABLE IF EXISTS osmdata;"])
            print(output)
        except subprocess.CalledProcessError as e:
            print(e.output)

    exit()

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
       
if __name__ == "__main__":
    main(sys.argv[1:])
