---
layout: doc
title:  "JMX Metric Monitoring" 
permalink: /docs/jmx-metric-monitoring.html
---

JMX metric for Apache Hadoop namenode url [http://127.0.0.1:50070/jmx](http://127.0.0.1:50070/jmx) can be monitored using Apache Eagle (called Eagle in the following). Follow below steps to enable this feature in Eagle.    

1. Install Python script (To populate JMX metric values to Kafka[^KAFKA] topic periodically).
2. Deploy "hadoopjmx" Storm[^STORM] topology.
3. Create new site and policy in UI.
4. Validate policy alert.

<br/>


### **Prerequisite**
* Complete the setup from [Setup Environment](/docs/deployment-env.html)	

<br/>


### **Setup**
From Hortonworks sandbox just run below setup script to Install Pyton JMX script, Create Kafka topic, update Apache Hbase tables and deploy "hadoopjmx" Storm topology. 

    $ /usr/hdp/current/eagle/examples/hadoop-metric-sandbox-starter.sh
    $ /usr/hdp/current/eagle/examples/hadoop-metric-policy-create.sh  

<br/>


### **Application Setup in UI**
1. Login to Eagle UI [http://localhost:9099/eagle-service/](http://localhost:9099/eagle-service/) using username and password as "admin" and "secret"
2. Click on "Admin" from top right and click "Management" button.
3. On Admin management page add "New Site" name "hadoopJmxMetricDataSource", by clicking on "New Site" link.
![add superuser](/images/docs/new-jmx-site.png)
4. Save the changes.
5. On eagle home page you should see new tab called "METRIC", beside "DAM".
6. Click on "JmxMetricMonitor" under "METRIC".
 
You should see policy with name "safeModePolicy".  

<br/>


### **Demo** 

* First make sure that Kafka topic "nn_jmx_metric_sandbox" is populated with JMX metric data periodically.(To make sure that python script is running)
 
        $ /usr/hdp/2.2.4.2-2/kafka/bin/kafka-console-consumer.sh --zookeeper sandbox.hortonworks.com:2181 --topic nn_jmx_metric_sandbox

* Genrate Alert by producing alert triggering message into Kafka topic.  


        $ /usr/hdp/2.2.4.2-2/kafka/bin/kafka-console-producer.sh --broker-list sandbox.hortonworks.com:6667 --topic nn_jmx_metric_sandbox
        $ {"host": "localhost", "timestamp": 1457033916718, "metric": "hadoop.namenode.fsnamesystemstate.fsstate", "component": "namenode", "site": "sandbox", "value": 1.0}


---

#### *Footnotes*

[^STORM]:*All mentions of "storm" on this page represent Apache Storm.*
[^KAFKA]:*All mentions of "kafka" on this page represent Apache Kafka.*
  
