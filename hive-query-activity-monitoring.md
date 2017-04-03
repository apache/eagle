---
layout: doc
title:  "Hive Query Activity Monitoring Quick Start" 
permalink: /docs/hive-query-activity-monitoring.html
---

*Since Apache Eagle 0.3.0-incubating. Apache Eagle will be called Eagle in the following.*

This Guide describes the steps to enable HIVE[^HIVE] query activity monitoring.

* Prerequisite
* Stream HIVE query logs into Eagle platform
* Demos "Hive Query Activity Monitoring"
<br/><br/>


### **Prerequisite**
* Complete the setup from [Quick Start(Eagle In Sandbox)](/docs/quick-start.html)	 
<br/><br/>


### **Stream HIVE query logs into Eagle platform**   
There are a couple of methods to capture HIVE query logs. As of 0.4.0, Eagle uses YARN API to periodically poll running HIVE jobs and in realtime parse query expressions. So here Eagle assumes resource manager is installed in Hadoop[^HADOOP] cluster. 

### **Demos**
* **Hive**:
	1. Click on menu "DAM" and select "Hive" to view Hive policy
	2. You should see policy with name "queryPhoneNumber". This Policy generates alert when hive table with sensitivity(Phone_Number) information is queried. 
	3. In sandbox read restricted sensitive HIVE column. ( To learn more about data sensitivity settings click [Data Classification Tutorial](/docs/tutorial/classification.html))

~~~
$ su hive
$ hive
$ set hive.execution.engine=mr;
$ use xademo;
$ select a.phone_number from customer_details a, call_detail_records b where a.phone_number=b.phone_number;
~~~

From UI click on alert tab and you should see alert for your attempt to read restricted column.  

---

#### *Footnotes*

[^HIVE]:*All mentions of "hive" on this page represent Apache Hive.*
