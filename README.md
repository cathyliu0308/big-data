# big-data
big data - hadoop projects

1. Build Hadoop Cluter on Docker (Linux)
$ mkdir bigdata
$ cd bigdata
$ sudo docker pull joway/hadoop-cluster
$ git clone https://github.com/joway/hadoop-cluster-docker
$ sudo docker network create --driver=bridge hadoop
$ cd hadoop-cluster-docker
$ sudo ./start-container.sh
$ ./start-hadoop.sh

2. run project on Docker:
$ cd /root/src/WordCount/src/main/java
$ hdfs dfs -mkdir /input
$ hdfs dfs -put inputFile.txt /input/ 
$ hadoop com.sun.tools.javac.Main *.java 
$ jar cf wordCount.jar *.class
$ hadoop jar wordCount.jar WordCount /input /output 
