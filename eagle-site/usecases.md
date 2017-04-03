---
layout: doc
title:  "Use Cases"
permalink: /docs/usecases.html
---

### Data Activity Monitoring

* Data activity represents how user explores data provided by big data platforms. Analyzing data activity and alerting for insecure access are fundamental requirements for securing enterprise data. As data volume is increasing exponentially with Hadoop[^HADOOP], Hive[^HIVE], Spark[^SPARK] technology, understanding data activities for every user becomes extremely hard,  let alone to alert for a single malicious event in real time among petabytes streaming data per day.

* Securing enterprise data starts from understanding data activities for every user. Apache Eagle (called Eagle in the following) has integrated with many popular big data platforms e.g. Hadoop, Hive, Spark, Cassandra[^CASSANDRA] etc. With Eagle user can browse data hierarchy, mark sensitive data and then create comprehensive policy to alert for insecure data access.

### Job Performance Analytics

* Running map/reduce job is the most popular way people use to analyze data in Hadoop system. Analyzing job performance and providing tuning suggestions are critical for Hadoop system stability, job SLA and resource usage etc. 

* Eagle analyzes job performance with two complementing approaches. First Eagle periodically takes snapshots for all running jobs with YARN API, secondly Eagle continuously reads job lifecycle events immediately after the job is completed. With the two approaches, Eagle can analyze single job's trend, data skew problem, failure reasons etc. More interestingly, Eagle can analyze whole Hadoop cluster's performance by taking into account all jobs.

### Node Anomaly Detection

* One of practical benefits from analyzing map/reduce job is node anomaly detection. Big data platform like Hadoop may involve thousands of nodes for supporting multi-tenant jobs. One bad node may not crash whole cluster thanks to failure tolerance design, but may affect specific jobs and cause a lot of rescheduling, job delay and hurt stability of whole cluster etc.

* Eagle developed out-of-the-box algorithm to compare task failure ratio for each node in a large cluster. If one node continues to fail running tasks, it may have potential issues, sometimes one of its disks is full or fails etc. In a nutshell, if one node behaves very differently from all other nodes within one large cluster, this node is anomalous and we should take action.

### Cluster Performance Analytics

* It is critical to understand why a cluster performs bad. Is that because of some crazy jobs recently onboarded, or huge amount of tiny files, or namenode performance degrading?

* Eagle in realtime calculates resource usage per minute out of individual jobs, e.g. CPU, memory, HDFS IO bytes, HDFS IO numOps etc. and also collects namenode JMX metrics. Correlating them together will easily help system adminstrator find root cause for cluster slowness.

### Cluster Resource Usage Trend

* YARN manages resource allocation through queue in a large Hadoop cluster. Cluster resource usage is exactly reflected by overall queue usage.

* Eagle in realtime collects queue statistics and provide insights of cluster resource usage.



---

#### *Footnotes*

[^HADOOP]:*All mentions of "hadoop" on this page represent Apache Hadoop.*
[^HIVE]:*All mentions of "hive" on this page represent Apache Hive.*
[^SPARK]:*All mentions of "spark" on this page represent Apache Spark.*
[^CASSANDRA]:*Apache Cassandra.*


