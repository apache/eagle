<!--
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
-->

> Application manager aims to manage topology status on EAGLE UI. Users can easily start/start topologies remotely or locally without any shell commands. At the same, it should be capable to sync the latest status of topologies on the execution platform (e.g., storm cluster). 

This tutorial will go through all parts of application manager and then give an example to use it. 

### Design
Application manager consists of a daemon scheduler and an execution module. The scheduler periodically loads user operations(start/stop) from database, and the execution module executes these operations. For more details, please refer to [here](https://cwiki.apache.org/confluence/display/EAG/Application+Management)

### Manual

#### Step 1: configure the scheduler
The configuration file `eagle-scheduler.conf` defines scheduler parameters, topology execution platform settings and parts of topology settings. Here are some important ones:

* **Scheduler properties**

    **appCommandLoaderEnabled**: enable application manager. **TODO**: change it to true. <br />
    **appCommandLoaderIntervalSecs**: defines the interval of the scheduler loads commands. Default value is 1 second.  <br />
    **appHealthCheckIntervalSecs**: define the interval of health check, which tries to sync the topology execution status to Eagle. <br /><br />

* **Execution platform properties**
   
    **envContextConfig.env**: application execution platform. Default value is storm.  <br />
    **envContextConfig.url**: execution platform http url. Default is "http://sandbox.hortonworks.com:8744".  <br />
    **envContextConfig.nimbusHost**: storm nimbus host. Default is "sandbox.hortonworks.com".  <br />
    **envContextConfig.nimbusThriftPort**: default is 6627.  
    **envContextConfig.jarFile**: storm fat jar path. **TODO**: change "/dir-to-jar/eagle-topology-0.3.0-incubating-assembly.jar" to your own jar path. <br /><br />

* **Topology default properties**
    
    Some default topology properties are defined here. It can be overridden.
   
After the configuration is ready, start Eagle service `bin/eagle-service.sh start`. 
  
#### Step 2: add topologies on UI
1. First of all, go to admin page 
   ![admin-page](/images/appManager/admin-page.png)
   ![topology-monitor](/images/appManager/topology-monitor.png)
    
2. Go to management page, and create a topology description. There are three required fields
    * name: topology name
    * type: topology type [CLASS, DYNAMIC]
    * execution entry: either the class which implement interface TopologyExecutable or eagle [DSL](https://github.com/apache/incubator-eagle/blob/master/eagle-assembly/src/main/conf/sandbox-hadoopjmx-pipeline.conf) based topology definition
   ![topology-description](/images/appManager/topology-description.png)
   
3. Back to monitoring page, and choose the site/application to deploy the topology 
   ![topology-execution](/images/appManager/topology-execution.png)
   
4. Go to site page, and edit site->application and add some new configurations. Blow are some example configurations for [site=sandbox, applicatoin=hbaseSecurityLog]
   `These configurations have a higher priority than those in eagle-scheduler.conf`
   
           classification.hbase.zookeeper.property.clientPort=2181
           classification.hbase.zookeeper.quorum=sandbox.hortonworks.com
           # platform related configurations
           app.envContextConfig.env=storm
           app.envContextConfig.mode=cluster
           # data source related configurations
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
           # service related configurations
           app.eagleProps.site=sandbox
           app.eagleProps.application=hbaseSecurityLog
           app.eagleProps.dataJoinPollIntervalSec=30
           app.eagleProps.mailHost=atom.corp.ebay.com
           app.eagleProps.mailSmtpPort=25
           app.eagleProps.mailDebug=true
           app.eagleProps.eagleService.host=localhost
           app.eagleProps.eagleService.port=9099
           app.eagleProps.eagleService.username=admin
           app.eagleProps.eagleService.password=secret
   ![topology-configuration-1](/images/appManager/topology-configuration-1.png)
   ![topology-configuration-2](/images/appManager/topology-configuration-2.png)
   
5. Go to monitoring page, and start topologies
   ![start-topology-1](/images/appManager/start-topology-1.png)
   ![start-topology-2](/images/appManager/start-topology-2.png)
   
6. stop topologies on monitoring page
   ![stop-topology-1](/images/appManager/stop-topology-1.png)
   ![stop-topology-2](/images/appManager/stop-topology-2.png)
   ![stop-topology-3](/images/appManager/stop-topology-3.png)

 