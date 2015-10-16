Eagle User Guide
========================

Prerequisites
-------------
* Hadoop
* HBase
* Storm
* Spark
* Kafka

Eagle requires you to have access on Hadoop CLI, where you have full permissions to HDFS, Storm, HBase and Kafka. To make things easier, we strongly recommend you to start Eagle on a hadoop sandbox such as http://hortonworks.com/products/hortonworks-sandbox/


Build
-----

* Download the latest version of Eagle source code.

		git clone git@github.xyz.com:eagle/eagle.git


* Build the source code, and a tar.gz package will be generated under eagle-assembly/target.

		mvn clean compile install -DskipTests

Installation
-----------
* Copy this package onto the sandbox.

		scp -P 2222 eagle/eagle-assembly/target/eagle-0.1.0-bin.tar.gz root@127.0.0.1:/usr/hdp/current/.

* Run Eagle patch installation at the first time, and restart HDFS namenode.

		bin/eagle-patch-install.sh


* Start Storm, HBase, and Kafka via Ambari Web UI. Make sure the user has the privilege to run Storm, HBase, and Kafka cmd in shell, and with full permissions to access HBase, such as creating tables. Check the installation & running status of the required services.

		bin/eagle-check-env.sh


* Create necessary HBase tables for Eagle.

		bin/eagle-service-init.sh


* Start Eagle service.

		bin/eagle-service.sh start
		

* Create Kafka topics and topology metadata for Eagle.

		bin/eagle-topology-init.sh


* Start Eagle topology, which will submit the topology to Storm via the Storm CLI tools. You can check it with storm UI.

		bin/eagle-topology.sh [--jar <jarName>] [--main <mainClass>] [--topology <topologyName>] start


Now you can let Eagle to monitor by creating your own policy!


Sandbox Starter
---------------

* startup Eagle service & topology

		examples/eagle-sandbox-starter.sh
		
* check eagle UI <http://127.0.0.1:9099/eagle-service>

* Take the following actions which will violate and obey the sample policy.
     * Violation Action: hdfs dfs -ls unknown
     * Violation Action: hdfs dfs -touchz /tmp/private
     * Obey Action: hdfs dfs -cat /tmp/private
