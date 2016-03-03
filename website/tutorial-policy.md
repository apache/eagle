---
layout: doc
title:  "Policy Tutorial" 
permalink: /docs/tutorial/policy.html
---

Eagle currently supports to customize policies for data sources for each site:

* HDFS Audit Log
* Hive Query Log

> NOTICE: policies are classified by sites. Please select the site first when there are multiple ones.

### How to define HDFS Policy?
In this example we will go through the steps for creating the following HDFS policy.

> Example Policy: Create a policy to alert when a user is trying to delete a file with sensitive data

* **Step 1**: Select Source as HDFS and Stream as HDFS Audit Log

	![HDFS Policies](/images/docs/hdfs-policy1.png)

* **Step 2**: Eagle supports a variety of properties for match critera where users can set different values. Eagle also supports window functions to extend policies with time functions.

	  command = delete 
	  (Eagle currently supports the following commands open, delete, copy, append, copy from local, get, move, mkdir, create, list, change permissions)
		
	  source = /tmp/private 
	  (Eagle supports wildcarding for property values for example /tmp/*)

	![HDFS Policies](/images/docs/hdfs-policy2.png)

* **Step 3**: Name your policy and select de-duplication options if you need to avoid getting duplicate alerts within a particular time window. You have an option to configure email notifications for the alerts.

	![HDFS Policies](/images/docs/hdfs-policy3.png)


### How to define HIVE Policy?
In this example we will go thru the steps for creating the following Hive policy.

> Example Policy: Create a policy to alert when a user is trying to select PHONE_NUMBER from a hive table with sensitive data

* **Step 1**:  Select Source as Hive and Stream as Hive Query Log

	![Hive Policies](/images/docs/hive-policy1.png)

* **Step 2**: Eagle support a variety of properties for match critera where users can set different values. Eagle also supports window functions to extend policies with time functions.

	  command = Select 
	  (Eagle currently supports the following commands DDL statements Create, Drop, Alter, Truncate, Show)
		
	  sensitivity type = PHONE_NUMBER
      (Eagle supports classifying data in Hive with different sensitivity types. Users can use these sensitivity types to create policies)

	![Hive Policies](/images/docs/hive-policy2.png)

* **Step 3**: Name your policy and select de-duplication options if you need to avoid getting duplicate alerts within a particular time window. You have an option to configure email notifications for the alerts.

	![Hive Policies](/images/docs/hive-policy3.png)
