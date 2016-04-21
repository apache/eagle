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


---
layout: doc
title:  "JMX Metric Monitoring" 
permalink: /docs/jmx-metric-monitoring.html
---
-->

JMX metric for hadoop namenode url [http://127.0.0.1:50070/jmx](http://127.0.0.1:50070/jmx) can be monitored using Eagle. Follow below steps to enable this feature in Eagle.    

1. Install Python script (To populate JMX metric values to Kafka message topic periodically.)
2. Deploy "hadoopjmx" storm topology.
3. Create new site and policy in UI
4. Validate policy alert.

<br/>


### **Prerequisite**
* Complete the setup from [sandbox-quick-start.md](https://github.com/hdendukuri/incubator-eagle/blob/master/eagle-docs/userguide/sandbox-quick-start.md)	

<br/>


### **Setup**
From Hortonworks sandbox just run below setup script to Install Pyton JMX script, Create Kafka topic, update Hbase tables and deploy "hadoopjmx" storm topology. 

    >$ /usr/hdp/current/eagle/examples/hadoop-metric-sandbox-starter.sh <br/>
    >$ /usr/hdp/current/eagle/examples/hadoop-metric-policy-create.sh <br/>

<br/>


### **Application Setup in UI**
1. Login to Eagle UI [http://localhost:9099/eagle-service/](http://localhost:9099/eagle-service/) username and password as "admin" and "secret"
2. Click on "Admin" from top right and click "Management" button.
3. On Admin management page add "New Site" name "hadoopJmxMetricDataSource" by clicking on "New Site" link.
![add superuser](https://github.com/hdendukuri/incubator-eagle/blob/master/eagle-docs/images/new-site.png)
4. Save the changes.
5. On eagle home page you should see new tab called "METRIC", besides "DAM".
6. Click on "JmxMetricMonitor" under "METRIC".
 
You should see safeModePolicy policy.  

<br/>


### **Demo** 

* First make sure that Kafka topic "nn_jmx_metric_sandbox" is populated with JMX metric data periodically.(To make sure that python script is running)
 
        >$ /usr/hdp/2.2.4.2-2/kafka/bin/kafka-console-consumer.sh --zookeeper sandbox.hortonworks.com:2181 --topic nn_jmx_metric_sandbox

* Genrate Alert by producing alert triggering message in Kafka topic.  


        >$ /usr/hdp/2.2.4.2-2/kafka/bin/kafka-console-producer.sh --broker-list sandbox.hortonworks.com:6667 --topic nn_jmx_metric_sandbox <br/>
        >$ {"host": "localhost", "timestamp": 1457033916718, "metric": "hadoop.namenode.fsnamesystemstate.fsstate", "component": "namenode", "site": "sandbox", "value": 1.0}
  
