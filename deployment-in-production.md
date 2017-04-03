---
layout: doc
title:  "Deploy Apache Eagle in the Production"
permalink: /docs/deployment-in-production.html
---


This page outlines the steps for deploying Apache Eagle (called Eagle in the following) in the production environment.

Here's the main content of this page:

* Setup Cluster Environment
* Start Eagle Service
   * Edit Configure files
   * Install metadata
* Rock with monitoring apps
* Stop Eagle Services


### **Setup Cluster Environment**
Eagle requires a setup cluster environment to run monitoring applications. For more details, please check [Environment](/docs/deployment-env.html) 
<br/>

### **Start Eagle Service**

* Step 1: Edit environment related configurations:

    * Edit `bin/eagle-env.sh`

            # TODO: make sure java version is 1.7.x
            export JAVA_HOME=

            # TODO: Apache Storm nimbus host. Default is localhost
            export EAGLE_NIMBUS_HOST=localhost

            # TODO: EAGLE_SERVICE_HOST, default is `hostname -f`
            export EAGLE_SERVICE_HOST=localhost


    * Edit `conf/eagle-service.conf` to configure the database to use (for example: HBase[^HBASE])

            # TODO: hbase.zookeeper.quorum in the format host1,host2,host3,...
            # default is "localhost"
            hbase-zookeeper-quorum="localhost"

            # TODO: hbase.zookeeper.property.clientPort
            # default is 2181
            hbase-zookeeper-property-clientPort=2181

            # TODO: hbase configuration: zookeeper.znode.parent
            # default is "/hbase"
            zookeeper-znode-parent="/hbase"

* Step 2: Install metadata for policies

        $ cd <eagle-home>

        # start Eagle web service
        $ bin/eagle-service.sh start

        # import metadata after Eagle service is successfully started
        $ bin/eagle-topology-init.sh

You have now successfully installed Eagle and setup a monitoring site. Next you can

* Setup a monitoring site [site management](/docs/tutorial/site.html)

* Create more policies with Eagle web [policy management](/docs/tutorial/policy.html)

* Enable resolver and classification functions of Eagle web [data classification](/docs/tutorial/classification.html)


### **Rock with Applications**

Currently Eagle provides several analytics solutions for identifying security on a Hadoop[^HADOOP] cluster. For example:

* [HDFS Data Activity Monitoring](/docs/hdfs-data-activity-monitoring.html)
* [HIVE Query Activity Monitoring](/docs/hive-query-activity-monitoring.html)[^HIVE]
* [HBASE Data Activity Monitoring](/docs/hbase-data-activity-monitoring.html)
* [MapR FS Data Activity Monitoring](/docs/mapr-integration.html)
* [Hadoop JMX Metrics Monitoring](/docs/jmx-metric-monitoring.html)

### **Stop Services**

* Stop eagle service

      $ bin/eagle-service.sh stop

---

#### *Footnotes*

[^HADOOP]:*All mentions of "hadoop" on this page represent Apache Hadoop.*
[^HBASE]:*All mentions of "hbase" on this page represent Apache HBase.*
[^HIVE]:*Apache Hive.*

