---
layout: doc
title:  "Deploy Eagle in the Production"
permalink: /docs/deployment-in-production.html
---


This page outlines the steps for deploying Eagle in the production environment.
Notice that Eagle requires you have full permissions to HDFS, HBase and Storm CLI.


Here's the main content of this page:

* Pre-requisites
   * Hadoop Environment
   * Stream HDFS audit log data into Kafka
* Installation
   * Edit Configure files
   * Install metadata
* Setup a monitoring site
   * Create a site with Eagle web
   * Create site related configuration files for topologies
   * Submit topologies
* Stop Eagle Services


### **Pre-requisites**

* **Hadoop Environment**

    * HDFS: 2.6.x
    * HBase: 0.98 or later
    * Storm: 0.9.3 or later
    * Kafka: 0.8.x or later
    * Java: 1.7.x


* **Stream HDFS audit log data (Only for HDFSAuditLog Monitoring)**

    1. Create a Kafka topic for importing audit log. Here is an example command to create topic sandbox_hdfs_audit_log.

           $ cd <kafka-home>
           $ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sandbox_hdfs_audit_log

    2. Populate audit log into the Kafka topic created above, and refer to [here](/docs/import-hdfs-auditLog.html) on How to do it.


### **Installation**

* Step 1: Edit environment related configurations:

    * Edit `bin/eagle-env.sh`

            # TODO: make sure java version is 1.7.x
            export JAVA_HOME=

            # TODO: Storm nimbus host. Default is localhost
            export EAGLE_NIMBUS_HOST=localhost

            # TODO: EAGLE_SERVICE_HOST, default is `hostname -f`
            export EAGLE_SERVICE_HOST=localhost


    * Edit `conf/eagle-service.conf`

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

        # create HBase tables
        $ bin/eagle-service-init.sh

        # start Eagle web service
        $ bin/eagle-service.sh start

        # import metadata after Eagle service is successfully started
        $ bin/eagle-topology-init.sh

### **Setup a monitoring site**

* Step 1: Login Eagle web http://${EAGLE_SERVICE_HOST}:9099/eagle-service with account `admin/secret`
        ![login](/images/docs/login.png)
* Step 2: Create a site with Eagle web
     (Example: create a site "Demo" with two data sources to monitor)
     ![setup a site](/images/docs/new-site.png)
* Step 3: Create site related configuration files for topologies

     Please refer to [samples](https://github.com/eBay/Eagle/tree/master/eagle-assembly/src/main/conf), and create a configuration file for each chosen datasource under $EAGLE_HOME/conf/.
        More descriptions are [here](/docs/configuration.html)
* Step 4: Submit topologies

          # start HDFS audilt log monitoring
          $ bin/eagle-topology.sh --main eagle.security.auditlog.HdfsAuditLogProcessorMain --config conf/XXXX-hdfsAuditLog-application.conf start

          # start Hive Query Log Monitoring
          $ bin/eagle-topology.sh --main eagle.security.hive.jobrunning.HiveJobRunningMonitoringMain --config conf/XXXX-hiveQueryLog-application.conf start

          # start User Profiles
          $ bin/eagle-topology.sh --main eagle.security.userprofile.UserProfileDetectionMain --config conf/XXXX-userprofile-topology.conf start

You have now successfully installed Eagle and setup a monitoring site. Next you can

* Create more policies with Eagle web [tutorial](/docs/tutorial/policy.html)

* Experience alerting with instructions on [Quick Starer](/docs/quick-start.html)

* Enable resolver and classification functions of Eagle web [tutorial](/docs/tutorial/setup.html)

* Check topologies with Storm UI

### **Stop Services**

* Stop eagle service

      $ bin/eagle-service.sh stop

* Stop eagle topologies

      $ bin/eagle-topology.sh --topology {topology-name} stop
      $ bin/eagle-topology.sh --topology {topology-name} stop
      $ bin/eagle-topology.sh --topology {topology-name} stop
