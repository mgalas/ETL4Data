# ETL Pipeline for OSM data set
## Set up

1. First create a directory in HDFS, on which user *mapred* has read and write permissions.

Example
```
sudo -u mapred hadoop fs -mkdir example
```
2. Create a directory called *lib* within the recently created directory.

Example
```
sudo -u mapred hadoop fs -mkdir example/lib
```
3. The Oozie workflow works with Java and Hive actions. Copy workflow.xml, job.properties and all hive scripts to 
directory *example* in HDFS.

Example
```
sudo -u mapred hadoop fs -put [file_to_upload] example
```
4. All Java programs must be compiled into jars, and it is recommended that all their dependencies are packaged in the jar as well.
Copy all jars to *example/lib*

5. Copy the hive configuration file hive-site.xml from /etc/hive/conf/hive-site.xml to the directory *example*  in HDFS.
6. To run the Oozie workkflow, execute the following command as mapred user:
```
sudo -u mapred oozie job -oozie [oozie-server_url]/oozie -config [path to job.properties in local file system] -run
```
Example
```
sudo -u mapred oozie job -oozie http://udltest2.cs.ucl.ac.uk:11000/oozie -config [path to job.properties in local file system] -run
```
