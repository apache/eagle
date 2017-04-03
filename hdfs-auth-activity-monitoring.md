---
layout: doc
title:  "HDFS Authorization Activity Monitoring Quick Start"
permalink: /docs/hdfs-auth-activity-monitoring.html
---

HDFS can audit service-level authorization activity 

#### Sample authorization logs

~~~
2016-06-08 02:55:07,742 INFO SecurityLogger.org.apache.hadoop.security.authorize.ServiceAuthorizationManager: Authorization successful for hdfs (auth:SIMPLE) for protocol=interface org.apache.hadoop.hdfs.protocol.ClientProtocol
2016-06-08 02:55:35,304 INFO SecurityLogger.org.apache.hadoop.security.authorize.ServiceAuthorizationManager: Authorization successful for hdfs (auth:SIMPLE) for protocol=interface org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol
2016-06-08 02:55:36,862 INFO SecurityLogger.org.apache.hadoop.security.authorize.ServiceAuthorizationManager: Authorization successful for hive (auth:SIMPLE) for protocol=interface org.apache.hadoop.hdfs.protocol.ClientProtocol
~~~

Steps for enabling service-level authorization activity

#### 1. Enable HDFS Authorization Security in core-site.xml

~~~
  <property>
      <name>hadoop.security.authorization</name>
      <value>true</value>
  </property>
~~~    

#### 2. Enable HDFS security log in log4j.properties
~~~
#
#Security audit appender
#
hadoop.security.logger=INFO,DRFAS
hadoop.security.log.maxfilesize=256MB
hadoop.security.log.maxbackupindex=20
log4j.category.SecurityLogger=${hadoop.security.logger}
hadoop.security.log.file=SecurityAuth.audit
log4j.appender.DRFAS=org.apache.log4j.DailyRollingFileAppender
log4j.appender.DRFAS.File=${hadoop.log.dir}/${hadoop.security.log.file}
log4j.appender.DRFAS.layout=org.apache.log4j.PatternLayout
log4j.appender.DRFAS.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n
log4j.appender.DRFAS.DatePattern=.yyyy-MM-dd
~~~
