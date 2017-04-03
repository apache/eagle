---
layout: doc
title:  "Quick Start" 
permalink: /docs/quick-start.html
---

Guide To Install **Apache Eagle 0.4.0-incubating** On Hortonworks sandbox. 

[For older versions: _Apache Eagle 0.3.0-incubating Quick Start_](/docs/quick-start-0.3.0.html)

* Setup Environment
* Download + Patch + Build
* Setup Hadoop[^HADOOP] Environment.
* Install Eagle.
* Sample Application: Hive[^HIVE] query activity monitoring in sandbox
<br/>

### **Setup Environment**
Eagle requires a streaming environment to run various applications. For more details, please check [Setup Environment](/docs/deployment-env.html)
<br/>

### **Download + Patch + Build**
* Download latest Eagle source released From Apache [[Tar]](https://dist.apache.org/repos/dist/release/eagle/apache-eagle-0.4.0-incubating/apache-eagle-0.4.0-incubating-src.tar.gz), [[MD5]](https://dist.apache.org/repos/dist/release/eagle/apache-eagle-0.4.0-incubating/apache-eagle-0.4.0-incubating-src.tar.gz.md5).
* Build manually with [Apache Maven](https://maven.apache.org/):

	  $ tar -zxvf apache-eagle-0.4.0-incubating-src.tar.gz
	  $ cd apache-eagle-0.4.0-incubating-src 
	  $ curl -O https://patch-diff.githubusercontent.com/raw/apache/eagle/pull/268.patch
	  $ git apply 268.patch
	  $ mvn clean package -DskipTests

	After building successfully, you will get a tarball under `eagle-assembly/target/` named `apache-eagle-0.4.0-incubating-bin.tar.gz`
<br/>

### **Install Eagle**
    
     $ scp -P 2222 eagle-assembly/target/apache-eagle-0.4.0-incubating-bin.tar.gz root@127.0.0.1:/root/
     $ ssh root@127.0.0.1 -p 2222 (password is hadoop)
     $ tar -zxvf apache-eagle-0.4.0-incubating-bin.tar.gz
     $ mv apache-eagle-0.4.0-incubating eagle
     $ mv eagle /usr/hdp/current/
     $ cd /usr/hdp/current/eagle
     $ examples/eagle-sandbox-starter.sh

<br/>

### **Sample Application: Hive query activity monitoring in sandbox**
After executing `examples/eagle-sandbox-starter.sh`, you have a sample application (topology) running on the Apache Storm (check with [storm ui](http://sandbox.hortonworks.com:8744/index.html)), and a sample policy of Hive activity monitoring defined.

Next you can trigger an alert by running a Hive query.

~~~
$ su hive
$ hive
$ set hive.execution.engine=mr;
$ use xademo;
$ select a.phone_number from customer_details a, call_detail_records b where a.phone_number=b.phone_number;
~~~
<br/>



---

#### *Footnotes*

[^HADOOP]:*Apache Hadoop.*
[^HIVE]:*All mentions of "hive" on this page represent Apache Hive.*

