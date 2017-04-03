---
layout: doc
title:  "Topology Management"
permalink: /docs/tutorial/topologymanagement.html
---
*Since Apache Eagle 0.4.0-incubating. Apache Eagle will be called Eagle in the following.*

> Application manager aims to manage applications on EAGLE UI. Users can easily start/start topologies remotely or locally without any shell commands. At the same, it should be capable to sync the latest status of topologies on the execution platform (e.g., Storm[^STORM] cluster). 

This tutorial will go through all parts of application manager and then give an example to use it. 

### Design
Application manager consists of a daemon scheduler and an execution module. The scheduler periodically loads user operations(start/stop) from database, and the execution module executes these operations. For more details, please refer to [here](https://cwiki.apache.org/confluence/display/EAG/Application+Management).

### Configurations
The configuration file `eagle-scheduler.conf` defines scheduler parameters, execution platform settings and parts of default topology configuration.

* **Scheduler properties**

    <style>
        table, td, th {
            border-collapse: collapse;
            border: 1px solid gray;
            padding: 10px;
        }
    </style>
    
    
    Property Name | Default  | Description  
    ------------- | :-------------:   | -----------  
    appCommandLoaderEnabled | false | topology management is enabled or not  
    appCommandLoaderIntervalSecs | 1  | defines the interval of the scheduler loads commands  
    appHealthCheckIntervalSecs | 5  | define the interval of topology health checking, which syncs the topology execution status from storm cluster to Eagle 
   


* **Execution platform properties**

    Property Name | Default  | Description  
    ------------- | :-------------   | -----------  
    envContextConfig.env | storm | execution environment, only storm is supported
    envContextConfig.url | http://sandbox.hortonworks.com:8744 | storm ui
    envContextConfig.nimbusHost | sandbox.hortonworks.com | storm nimbus host
    envContextConfig.nimbusThriftPort | 6627  | storm nimbus thrift port  
    envContextConfig.jarFile | TODO  | storm fat jar path

* **Topology default properties**
    
    Some default topology properties are defined here. 
   
  
### Playbook

1. Editing eagle-scheduler.conf, and start Eagle service

        # enable application manager       
        appCommandLoaderEnabled = true
        
        # provide jar path
        envContextConfig.jarFile =
        
        # storm nimbus
        envContextConfig.url = "http://sandbox.hortonworks.com:8744"
        envContextConfig.nimbusHost = "sandbox.hortonworks.com"
        
        
        
   
    For more configurations, please back to [Application Configuration](/docs/configuration.html). <br />
    After the configuration is ready, start Eagle service `bin/eagle-service.sh start`. 
   
2. Go to admin page 
   ![admin-page](/images/appManager/admin-page.png)
   ![topology-monitor](/images/appManager/topology-monitor.png)
    
3. Go to management page, and create a topology description. There are three required fields
    * name: topology name
    * type: topology type [CLASS, DYNAMIC]
    * execution entry: either the class which implements interface TopologyExecutable or eagle [DSL](https://github.com/apache/eagle/blob/master/eagle-assembly/src/main/conf/sandbox-hadoopjmx-pipeline.conf) based topology definition
   ![topology-description](/images/appManager/topology-description.png)
   
4. Back to monitoring page, and choose the site/application to deploy the topology 
   ![topology-execution](/images/appManager/topology-execution.png)
   
5. Go to site page, and add topology configurations. 
   
   **NOTICE** topology configurations defined here are REQUIRED an extra prefix `.app`
   
   Blow are some example configurations for [site=sandbox, applicatoin=hbaseSecurityLog]. 
   

  
        classification.hbase.zookeeper.property.clientPort=2181
        classification.hbase.zookeeper.quorum=sandbox.hortonworks.com
        
        app.envContextConfig.env=storm
        app.envContextConfig.mode=cluster
        
        app.dataSourceConfig.topic=sandbox_hbase_security_log
        app.dataSourceConfig.zkConnection=sandbox.hortonworks.com:2181
        app.dataSourceConfig.zkConnectionTimeoutMS=15000
        app.dataSourceConfig.brokerZkPath=/brokers
        app.dataSourceConfig.fetchSize=1048586
        app.dataSourceConfig.transactionZKServers=sandbox.hortonworks.com
        app.dataSourceConfig.transactionZKPort=2181
        app.dataSourceConfig.transactionZKRoot=/consumers
        app.dataSourceConfig.consumerGroupId=eagle.hbasesecurity.consumer
        app.dataSourceConfig.transactionStateUpdateMS=2000
        app.dataSourceConfig.deserializerClass=org.apache.eagle.security.hbase.parse.HbaseAuditLogKafkaDeserializer
        
        app.eagleProps.site=sandbox
        app.eagleProps.application=hbaseSecurityLog
        app.eagleProps.dataJoinPollIntervalSec=30
        app.eagleProps.mailHost=some.mail.server
        app.eagleProps.mailSmtpPort=25
        app.eagleProps.mailDebug=true
        app.eagleProps.eagleService.host=localhost
        app.eagleProps.eagleService.port=9099
        app.eagleProps.eagleService.username=admin
        app.eagleProps.eagleService.password=secret
    

   ![topology-configuration-1](/images/appManager/topology-configuration-1.png)
   ![topology-configuration-2](/images/appManager/topology-configuration-2.png)
   
6. Go to monitoring page, and start topologies
   ![start-topology-1](/images/appManager/start-topology-1.png)
   ![start-topology-2](/images/appManager/start-topology-2.png)
   
7. stop topologies on monitoring page
   ![stop-topology-1](/images/appManager/stop-topology-1.png)
   ![stop-topology-2](/images/appManager/stop-topology-2.png)
   ![stop-topology-3](/images/appManager/stop-topology-3.png)




---

#### *Footnotes*

[^STORM]:*All mentions of "storm" on this page represent Apache Storm.*

