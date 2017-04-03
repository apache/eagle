---
layout: doc
title:  "HBase Authorization Activity Monitoring Quick Start"
permalink: /docs/hbase-auth-activity-monitoring.html
---

HBase[^HBASE] audits authorization activities in log files

Please follow below steps to enable HBase authorization auditing in HDP sandbox and Cloudera

#### 1. in hbase-site.xml

Note: when testing in HDP sandbox, sometimes Apache Ranger will take over access controll for HBase, so maybe you need change that back to native hbase access controller, i.e. change com.xasecure.authorization.hbase.XaSecureAuthorizationCoprocessor to org.apache.hadoop.hbase.security.access.AccessController

~~~
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
     <value>org.apache.hadoop.hbase.security.token.TokenProvider,org.apache.hadoop.hbase.security.access.AccessController</value>
</property>
~~~

#### 2. log4j.properties

~~~
#
# Security audit appender
#
hbase.security.log.file=SecurityAuth.audit
hbase.security.log.maxfilesize=256MB
hbase.security.log.maxbackupindex=20
log4j.appender.RFAS=org.apache.log4j.RollingFileAppender
log4j.appender.RFAS.File=${hbase.log.dir}/${hbase.security.log.file}
log4j.appender.RFAS.MaxFileSize=${hbase.security.log.maxfilesize}
log4j.appender.RFAS.MaxBackupIndex=${hbase.security.log.maxbackupindex}
log4j.appender.RFAS.layout=org.apache.log4j.PatternLayout
log4j.appender.RFAS.layout.ConversionPattern=%d{ISO8601} %p %c: %m%n
log4j.category.SecurityLogger=${hbase.security.logger}
log4j.additivity.SecurityLogger=false
log4j.logger.SecurityLogger.org.apache.hadoop.hbase.security.access.AccessController=TRACE
~~~

---

#### *Footnotes*

[^HBASE]:*All mentions of "hbase" on this page represent Apache HBase.*
