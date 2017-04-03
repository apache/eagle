---
layout: doc
title:  "HBase Data Activity Monitoring" 
permalink: /docs/hbase-data-activity-monitoring.html
---

*On this page, any mention of Eagle points to Apache Eagle.*

This page will introduce how to monitoring HBase data activity in the following aspects:

* How to enable HBase[^HBASE] security audit log
* How to add a Kafka[^KAFKA] log4j appender
* How to enable Eagle HBase monitoring

### How to enable HBase security audit log

> Notice: if you are willing to use sample logs under eagle-security-hbase-security/test/resources/securityAuditLog, please skip this part.

1. edit Advanced hbase-log4j via Ambari[^AMBARI] UI, and append below sentence to `Security audit appender`

        log4j.logger.SecurityLogger.org.apache.hadoop.hbase.security.access.AccessController=TRACE,RFAS

2. edit Advanced hbase-site.xml
    
        <property>
          <name>hbase.security.authorization</name>
          <value>true</value>
        </property>

        <property>
          <name>hbase.coprocessor.master.classes</name>
          <value>org.apache.hadoop.hbase.security.access.AccessController</value>
        </property>

        <property>
          <name>hbase.coprocessor.region.classes</name>
          <value>org.apache.hadoop.hbase.security.access.AccessController</value>
        </property>

3. Save and restart HBase
 
Now you can check /var/log/hbase/SecurityAudit.log whether the log appears. 

### How to add a Kafka log4j appender

> Notice: if you are willing to use sample logs under `eagle-security-hbase-security/test/resources/securityAuditLog`, please skip this part.

1. create Kafka topic `sandbox_hbase_security_log`

        $ /usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sandbox_hbase_security_log
2. add below “KAFKA_HBASE_AUDIT” log4j appender to `Security audit appender`
   Please refer to http://goeagle.io/docs/import-hdfs-auditLog.html.
   
        log4j.appender.KAFKA_HBASE_AUDIT=org.apache.eagle.log4j.kafka.KafkaLog4jAppender
        log4j.appender.KAFKA_HBASE_AUDIT.Topic=sandbox_hbase_security_log
        log4j.appender.KAFKA_HBASE_AUDIT.BrokerList=sandbox.hortonworks.com:6667
        log4j.appender.KAFKA_HBASE_AUDIT.Layout=org.apache.log4j.PatternLayout
        log4j.appender.KAFKA_HBASE_AUDIT.Layout.ConversionPattern=%d{ISO8601} %p %c: %m%n
        log4j.appender.KAFKA_HBASE_AUDIT.ProducerType=async
        log4j.appender.KAFKA_HDFS_AUDIT.KeyClass=org.apache.eagle.log4j.kafka.hadoop.GenericLogKeyer
        log4j.appender.KAFKA_HDFS_AUDIT.KeyPattern=user=(\\w+),\\s+

3. add the reference to KAFKA_HBASE_AUDIT to log4j appender
 
        log4j.logger.SecurityLogger.org.apache.hadoop.hbase.security.access.AccessController=TRACE,RFAS,KAFKA_HBASE_AUDIT

4. add Eagle log4j appender jars into HBASE_CLASSPATH BY editing Advanced hbase-env via Ambari UI

        export HBASE_CLASSPATH=${HBASE_CLASSPATH}:/usr/hdp/current/eagle/lib/log4jkafka/lib/*

5. Save and restart HBase

### How to enable Eagle hBase monitoring

1. create tables (`skip if you do not use hbase`)

        bin/eagle-service-init.sh 

2. start Eagle service 

        bin/eagle-service.sh start

3. import metadata 
 
        bin/eagle-topology-init.sh

4. submit topology

        bin/eagle-topology.sh --main org.apache.eagle.security.hbase.HbaseAuditLogProcessorMain --config conf/sandbox-hbaseSecurityLog-application.conf start

(sample sensitivity data at `examples/sample-sensitivity-resource-create.sh`)

### Q & A

Q1: found "java.lang.ClassNotFoundException: org.apache.eagle.log4j.kafka.KafkaLog4jAppender" in /var/log/hbase/hbase-hbase-master-sandbox.hortonworks.com.out

A1: 1) make sure the jars have been included in HBASE_CLASSPATH (run hbase classpath in the shell). 2) make sure this jars can be executed by other users. 3) check /etc/hbase/conf/hbase-site.xml whether there is newline between two properties. 


---

#### *Footnotes*

[^HBASE]:*All mentions of "hbase" on this page represent Apache HBase.*
[^KAFKA]:*All mentions of "kafka" on this page represent Apache Kafka.*
[^AMBARI]:*All mentions of "ambari" on this page represent Apache Ambari.*



    