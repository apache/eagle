<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

Guide To Install Eagle Hortonworks sandbox. 
===============================================
* Prerequisite
* Download + Patch + Build
* Setup Hadoop Environment In Sandbox
* Injection of hdfs-audit.logs to Kafka.
* Install Eagle In Sandbox
* Demos "HDFS File System" & "Hive" monitoring

### **Prerequisite**
* To install Eagle on a sandbox you need to run a HDP sandbox image in a virtual machine with 8GB memory recommended.
	1. Get Virtual Box or VMware [Virtualization environment](http://hortonworks.com/products/hortonworks-sandbox/#install)
	2. Get [Hortonworks Sandbox v 2.2.4](http://hortonworks.com/products/hortonworks-sandbox/#archive)
* JDK 1.7  
* NPM (On MAC OS try "brew install node") 	
<br/>

### **Download + Patch + Build**
* Download latest Eagle source released From Apache [[Tar]](http://www-us.apache.org/dist/incubator/eagle/apache-eagle-0.3.0-incubating/apache-eagle-0.3.0-incubating-src.tar.gz) , [[MD5]](http://www-us.apache.org/dist/incubator/eagle/apache-eagle-0.3.0-incubating/apache-eagle-0.3.0-incubating-src.tar.gz.md5) 
* Build manually with [Apache Maven](https://maven.apache.org/):

     >$ tar -zxvf apache-eagle-0.3.0-incubating-src.tar.gz <br/>
     >$ cd incubator-eagle-release-0.3.0-rc3 <br/>
     >$ curl -O https://patch-diff.githubusercontent.com/raw/apache/incubator-eagle/pull/150.patch <br/>
     >$ git apply 150.patch <br/>
     >$ mvn clean package -DskipTests <br/>
     
* After building successfully, you will get the tarball under `eagle-assembly/target/` named as `eagle-0.3.0-incubating-bin.tar.gz`
<br/>

### **Setup Hadoop Environment In Sandbox**
1. Launch Ambari to manage the Hadoop environment
   * Enable Ambari in sandbox http://127.0.0.1:8000 (Click on Enable Button)
   * Login to Ambari UI http://127.0.0.1:8080/ with username and password as "admin"
2. Grant root as HBase superuser via Ambari
![add superuser](https://github.com/hdendukuri/incubator-eagle/blob/master/eagle-docs/images/hbase-superuser.png)
3. Start HBase,Storm & Kafka from Ambari.Showing Storm as an example below. 
![Restart Services](https://github.com/hdendukuri/incubator-eagle/blob/master/eagle-docs/images/start-storm.png "Services")
4. If the NAT network is used in a virtual machine, it's required to add port 9099 to forwarding ports
  ![Forwarding Port](https://github.com/hdendukuri/incubator-eagle/blob/master/eagle-docs/images/eagle-service.png)

<br/>

### **Injection of hdfs-audit.logs to Kafka (This section is only for "HDFS File System" Monitoring)**   
Stream HDFS audit log into Kafka.

* **Step 1**: Configure Advanced hadoop-log4j via <a href="http://localhost:8080/#/main/services/HDFS/configs" target="_blank">Ambari UI</a>, and add below "KAFKA_HDFS_AUDIT" log4j appender to hdfs audit logging.

	log4j.appender.KAFKA_HDFS_AUDIT=org.apache.eagle.log4j.kafka.KafkaLog4jAppender
	log4j.appender.KAFKA_HDFS_AUDIT.Topic=sandbox_hdfs_audit_log
	log4j.appender.KAFKA_HDFS_AUDIT.BrokerList=sandbox.hortonworks.com:6667
	log4j.appender.KAFKA_HDFS_AUDIT.KeyClass=org.apache.eagle.log4j.kafka.hadoop.AuditLogKeyer
	log4j.appender.KAFKA_HDFS_AUDIT.Layout=org.apache.log4j.PatternLayout
	log4j.appender.KAFKA_HDFS_AUDIT.Layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n
	log4j.appender.KAFKA_HDFS_AUDIT.ProducerType=async

    ![HDFS LOG4J Configuration](https://github.com/hdendukuri/incubator-eagle/blob/master/eagle-docs/images/hdfs-log4j-conf.png "hdfslog4jconf")

* **Step 2**: Edit Advanced hadoop-env via <a href="http://localhost:8080/#/main/services/HDFS/configs" target="_blank">Ambari UI</a>, and add the reference to KAFKA_HDFS_AUDIT to HADOOP_NAMENODE_OPTS.

      -Dhdfs.audit.logger=INFO,DRFAAUDIT,KAFKA_HDFS_AUDIT

    ![HDFS Environment Configuration](https://github.com/hdendukuri/incubator-eagle/blob/master/eagle-docs/images/hdfs-env-conf.png "hdfsenvconf")

* **Step 3**: Edit Advanced hadoop-env via <a href="http://localhost:8080/#/main/services/HDFS/configs" target="_blank">Ambari UI</a>, and append the following command to it.

      export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/usr/hdp/current/eagle/lib/log4jkafka/lib/*

    ![HDFS Environment Configuration](https://github.com/hdendukuri/incubator-eagle/blob/master/eagle-docs/images/hdfs-env-conf2.png "hdfsenvconf2")

* **Step 4**: save the changes 

* **Step 5**: "Restart All" Storm & Kafka from Ambari. (Similar to starting server in pervious step except make sure to click on "Restart All")

* **Step 6**: Restart name node 
![Restart Services](https://github.com/hdendukuri/incubator-eagle/blob/master/eagle-docs/images/nn-restart.png "Services")

* **Step 7**: Check whether logs from are flowing into topic `sandbox_hdfs_audit_log`
      
      > /usr/hdp/2.2.4.2-2/kafka/bin/kafka-console-consumer.sh --zookeeper sandbox.hortonworks.com:2181 --topic sandbox_hdfs_audit_log      
      
<br/>

### **Install Eagle In Sandbox**
The following installation actually contains installing and setting up a sandbox site with HdfsAuditLog & HiveQueryLog  data sources. 
    
    >$ scp -P 2222  /eagle-assembly/target/eagle-0.3.0-incubating-bin.tar.gz root@127.0.0.1:/root/ <br/>
    >$ ssh root@127.0.0.1 -p 2222 <br/>
    >$ tar -zxvf eagle-0.3.0-incubating-bin.tar.gz <br/>
    >$ mv eagle-0.3.0-incubating eagle <br/>
    >$ mv eagle /usr/hdp/current/ <br/>
    >$ cd /usr/hdp/current/eagle <br/>
    >$ examples/eagle-sandbox-starter.sh <br/>

<br/>

### **Demos**
* Login to Eagle UI [http://localhost:9099/eagle-service/](http://localhost:9099/eagle-service/) username and password as "admin" and "secret"
* **HDFS**:
	1. Click on menu "DAM" and select "HDFS" to view HDFS policy
	2. You should see policy with name "viewPrivate". This Policy generates alert when any user reads HDFS file name "private" under "tmp" folder.
	3. In sandbox read restricted HDFS file "/tmp/private" by using command 
	
	   > hadoop fs -cat "/tmp/private"

	From UI click on alert tab and you should see alert for the attempt to read restricted file.  
* **Hive**:
	1. Click on menu "DAM" and select "Hive" to view Hive policy
	2. You should see policy with name "queryPhoneNumber". This Policy generates alert when hive table with sensitivity(Phone_Number) information is queried. 
	3. In sandbox read restricted sensitive HIVE column.
	
       >$ su hive <br/>
       >$ hive <br/>
       >$ set hive.execution.engine=mr; <br/>
       >$ use xademo; <br/>
       >$ select a.phone_number from customer_details a, call_detail_records b where a.phone_number=b.phone_number; <br/>

   4. From UI click on alert tab and you should see alert for your attempt to dfsf read restricted column.  

<br/>
