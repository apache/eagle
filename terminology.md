---
layout: doc
title:  "Terminology" 
permalink: /docs/terminology.html
---

Here are some terms we are using in Apache Eagle (called Eagle in the following), please check them for your reference.
They are basic knowledge of Eagle which also will help to well understand Eagle.

* **Site**: a site can be considered as a physical data center. Big data platform e.g. Hadoop[^HADOOP] may be deployed to multiple data centers in an enterprise. 

* **Application**: an application is composed of data integration, policies and insights for one data source.

* **Policy**: a policy defines the rule to alert. Policy can be simply a filter expression or a complex window based aggregation rules etc. 

* **Data source**: a data source is a monitoring target data. Eagle supports many data sources HDFS audit logs, Hive[^HIVE] query, MapReduce job etc.

* **Stream**: a stream is the streaming data from a data source. Each data source has its own stream.

* **Data activity monitoring**: Data activity monitoring is to monitor how user exploits data in Hadoop system etc. 

* **User profile**: a user profile is the historical activity model generated using machine learning algorithm which could be used for showing insights.

* **Data classification**: data classification provides the ability to classify different data sources with different levels of sensitivity.



---

#### *Footnotes*

[^HADOOP]:*All mentions of "hadoop" on this page represent Apache Hadoop.*
[^HIVE]:*Apache Hive.*

