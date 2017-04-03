---
layout: doc
title:  "User Profile Tutorial"
permalink: /docs/tutorial/userprofile.html
---
This document will introduce how to start the online processing on user profiles. Assume Apache Eagle has been installed and [Eagle service](http://sandbox.hortonworks.com:9099/eagle-service)
is started.

### User Profile Offline Training

* **Step 1**: Start Apache Spark if not started
![Start Spark](/images/docs/start-spark.png)

* **Step 2**: start offline scheduler

	* Option 1: command line

	      $ cd <eagle-home>/bin
	      $ bin/eagle-userprofile-scheduler.sh --site sandbox start

	* Option 2: start via Apache Ambari
	![Click "ops"](/images/docs/offline-userprofile.png)

* **Step 3**: generate a model

	![Click "ops"](/images/docs/userProfile1.png)
	![Click "Update Now"](/images/docs/userProfile2.png)
	![Click "Confirm"](/images/docs/userProfile3.png)
	![Check](/images/docs/userProfile4.png)

### User Profile Online Detection

Two options to start the topology are provided.

* **Option 1**: command line

	submit userProfiles topology if it's not on [topology UI](http://sandbox.hortonworks.com:8744)

      $ bin/eagle-topology.sh --main org.apache.eagle.security.userprofile.UserProfileDetectionMain --config conf/sandbox-userprofile-topology.conf start

* **Option 2**: Apache Ambari
	
	![Online userProfiles](/images/docs/online-userprofile.png)

### Evaluate User Profile in Sandbox

1. Prepare sample data for ML training and validation sample data
* a. Download following sample data to be used for training 
	* [`user1.hdfs-audit.2015-10-11-00.txt`](/data/user1.hdfs-audit.2015-10-11-00.txt) 
	* [`user1.hdfs-audit.2015-10-11-01.txt`](/data/user1.hdfs-audit.2015-10-11-01.txt)
* b. Downlaod [`userprofile-validate.txt`](/data/userprofile-validate.txt)file which contains data points that you can try to test the models

2. Copy the files (downloaded in the previous step) into a location in sandbox 
For example: `/usr/hdp/current/eagle/lib/userprofile/data/`
3. Modify `<Eagle-home>/conf/sandbox-userprofile-scheduler.conf `
update `training-audit-path` to set to the path for training data sample (the path you used for Step 1.a)
update detection-audit-path to set to the path for validation (the path you used for Step 1.b)
4. Run ML training program from eagle UI
5. Produce Apache Kafka data using the contents from validate file (Step 1.b)
Run the command (assuming the eagle configuration uses Kafka topic `sandbox_hdfs_audit_log`) 

		./kafka-console-producer.sh --broker-list sandbox.hortonworks.com:6667 --topic sandbox_hdfs_audit_log

6. Paste few lines of data from file validate onto kafka-console-producer 
Check [http://localhost:9099/eagle-service/#/dam/alertList](http://localhost:9099/eagle-service/#/dam/alertList) for generated alerts 
