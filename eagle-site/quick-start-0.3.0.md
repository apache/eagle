---
layout: doc
title:  "Quick Start" 
permalink: /docs/quick-start-0.3.0.html
---

Guide To Install Apache Eagle 0.3.0-incubating to Hortonworks sandbox.  
*Apache Eagle will be called Eagle in the following.*

* Prerequisite
* Download + Patch + Build
* Setup Hadoop[^HADOOP] Environment.
* Install Eagle.
* Demo
<br/>

### **Prerequisite**
Eagle requires a streaming environment to run various applications. For more details, please check [Setup Environment](/docs/deployment-env.html)
<br/>

### **Download + Patch + Build**
* Download Eagle 0.3.0 source released From Apache [[Tar]](https://dist.apache.org/repos/dist/release/eagle/apache-eagle-0.3.0-incubating/apache-eagle-0.3.0-incubating-src.tar.gz) , [[MD5]](https://dist.apache.org/repos/dist/release/eagle/apache-eagle-0.3.0-incubating/apache-eagle-0.3.0-incubating-src.tar.gz.md5) 
* Build manually with [Apache Maven](https://maven.apache.org/):

	  $ tar -zxvf apache-eagle-0.3.0-incubating-src.tar.gz
	  $ cd incubator-eagle-release-0.3.0-rc3  
	  $ curl -O https://patch-diff.githubusercontent.com/raw/apache/eagle/pull/180.patch
	  $ git apply 180.patch
	  $ mvn clean package -DskipTests

	After building successfully, you will get tarball under `eagle-assembly/target/` named as `eagle-0.3.0-incubating-bin.tar.gz`
<br/>

### **Install Eagle**
    
     $ scp -P 2222  eagle-assembly/target/eagle-0.3.0-incubating-bin.tar.gz root@127.0.0.1:/root/
     $ ssh root@127.0.0.1 -p 2222 (password is hadoop)
     $ tar -zxvf eagle-0.3.0-incubating-bin.tar.gz
     $ mv eagle-0.3.0-incubating eagle
     $ mv eagle /usr/hdp/current/
     $ cd /usr/hdp/current/eagle
     $ examples/eagle-sandbox-starter.sh

<br/>

### **Demos**
* Login to Eagle UI [http://localhost:9099/eagle-service/](http://localhost:9099/eagle-service/) using username and password as "admin" and "secret"
* [HDFS & Hive](/docs/hdfs-hive-monitoring.html)
<br/>



---

#### *Footnotes*

[^HADOOP]:*All mentions of "hadoop" on this page represent Apache Hadoop.*
