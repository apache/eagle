---
layout: doc
title:  "Development in Intellij"
permalink: /docs/development-in-intellij.html
---

Apache Eagle (called Eagle in the following) can be developed in popular IDE, e.g. Intellij and Eclipse. Here we focus on development in Intellij.

### 1. Prepare Hadoop[^HADOOP] environment

Normally HDP sandbox is needed for testing Hadoop monitoring. Please reference [Quick Start](/docs/quick-start.html) for setting up HDP sandbox.

Please ensure following daemons are up

* Namenode

* Datanode

* HBase[^HBASE] master/region server only needed when HBase is metadata store

* MySQL only neede when MySQL is metadata store

* Zookeeper[^ZOOKEEPER]

* Kafka[^KAFKA]

### 2. Start Eagle web service in Intellij

Import source code into Intellij, and find eagle-webservice project. Intellij Ultimate supports launching J2EE server within Intellij. If you don't have 
Intellij Ultimate version, Eclipse is another option. 

* **Check service configuration**

Under eagle-webservice/src/main/resources, configure application.conf for metadata store. As of Eagle 0.4.0, by default, it points to sandbox HBase. If you want to use MySQL as metadata store, just copy application-mysql.conf to application.conf.

* **Ensure tables are created**

Run eagle-service-init.sh under eagle-assembly/src/main/bin/eagle-service-init.sh to create metadata tables.

* **Start Eagle service**

Configure Intellij for running Apache Tomcat server with eagle-service artifacts

### 3. Start topology in Intellij

* **Check topology configuration**

Take HDFS data activity monitoring as example, config application.conf under eagle-security/eagle-security-hdfs-securitylog/src/main/resources for eagle service endpoint, Kafka endpoint etc.

* **Start Storm[^STORM] topology**

Normally one data source has one correponding Storm topology, for example to monitor HDFS data activity, org.apache.eagle.security.auditlog.HdfsAuditLogProcessorMain is used for starting Storm topology and reading audit logs from Kafka.

Other example data integrations are as follows

Hive query activity monitoring : org.apache.eagle.security.hive.jobrunning.HiveJobRunningMonitoringMain

HBase data activity monitoring: org.apache.eagle.security.hbaseHbaseAuditLogMonitoringTopology

MapR FS data activity monitoring: org.apache.eagle.security.auditlog.MapRFSAuditLogProcessorMain

---

#### *Footnotes*

[^HADOOP]:*All mentions of "hadoop" on this page represent Apache Hadoop.*
[^HBASE]:*All mentions of "hbase" on this page represent Apache HBase.*
[^HIVE]:*Apache Hive.*
[^KAFKA]:*All mentions of "kafka" on this page represent Apache Kafka.*
[^STORM]:*All mentions of "storm" on this page represent Apache Storm.*
[^ZOOKEEPER]:*Apache ZooKeeper.*

