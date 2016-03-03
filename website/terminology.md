---
layout: doc
title:  "Terminology" 
permalink: /docs/terminology.html
---

Here are some terms we are using in Apache Eagle, please check them for your reference.
They are basic knowledge of Eagle which also will help to well understand Eagle.

* **Site**: a site can be considered as a Hadoop environment. Eagle distinguishes different Hadoop environment with different sites.
* **Policy**: a policy defines the rule to alert. Users can define their own policies based on the metadata of data sources
* **Data source**: a data source is a monitoring target data. Currently Eagle only supports two kinds of data sources: Hive query log and HDFS audit log.
* **Stream**: a stream is the streaming data from a data source. Each data source has its own stream.
* **Data activity monitoring**: Data activity monitoring is to monitor the data from each data source and to alert according the policy defined by users.
* **User profile**: a user profile is the historical activity model generated using machine learning algorithm which could be used for real-time online abnormal detection.
* **Data classification**: data classification provides the ability to classify different data sources with different levels of sensitivity.
