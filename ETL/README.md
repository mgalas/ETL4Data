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

## Useful links
- [Oozie examples](http://www.tanzirmusabbir.com/2013/03/oozie-example-hive-actions.html)
- [Oozie libraries](http://blog.cloudera.com/blog/2014/05/how-to-use-the-sharelib-in-apache-oozie-cdh-5/)
- [OSM to Hive parser](https://github.com/PanierAvide/BasicOSMParser)
- [Oozie shell and Java actions](https://blog.cloudera.com/blog/2013/03/how-to-use-oozie-shell-and-java-actions/)
- [Oozie scheduler](https://blog.cloudera.com/blog/2013/01/how-to-schedule-recurring-hadoop-jobs-with-apache-oozie/)
- [Running Jupter Notebook on Cloudera](https://blogs.msdn.microsoft.com/pliu/2016/06/19/run-jupyter-notebook-on-cloudera/)
- [OSM quality assurance](https://github.com/keepright/keepright/tree/master/checks)
- [MiniOozie](https://oozie.apache.org/docs/3.2.0-incubating/ENG_MiniOozie.html)
