---
layout: doc
title:  "Cloudera Integration"
permalink: /docs/cloudera-integration.html
---

*Since Eagle 0.4.0*

Configuring Apache Eagle on Cloudera is very similar to configuring it on Hortonworks, but still there are some difference. 
This tutorial is to address these issues before you continue to follow the tutorials originally prepared for Hortonworks.

### Prerequisites

To get Apache Eagle works on Cloudera, we need:

* Zookeeper (installed through Cloudera Manager)
* Kafka (installed through Cloudera Manager)
* Storm (``0.9.x`` or ``0.10.x``, installed manually)
* Logstash (``2.X``, installed manually on NameNode)


### Kafka

#### Configuration

There are two configurations needed to be mentioned: 

* Open Cloudera Manager and open "kafka" configuration, then set ``“zookeeper Root”`` to ``“/”``.

* If Kafka cannot be started successfully, check kafka’s log. If stack trace shows: ``“java.lang.OutOfMemoryError: Java heap space”``. Increase heap size by setting ``"KAFKA_HEAP_OPTS"``in ``/bin/kafka-server-start.sh``.

Example:

~~~
                  export KAFKA_HEAP_OPTS="-Xmx2G -Xms2G"
~~~



#### Verification

- Step1: create a kafka topic (here I created a topic called “test”, which will be used in  logstash configuration file to receive hdfsAudit log messages from Cloudera.

~~~
bin/kafka-topics.sh --create --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 1 --topic test
~~~

- Step2: check if topic has been created successfully.

~~~
bin/kafka-topics.sh --list --zookeeper 127.0.0.1:2181
~~~

this command will show all created topics.

- Step3: open two terminals, start “producer” and “consumer” separately.

~~~
/usr/bin/kafka-console-producer --broker-list hostname:9092 --topic test
/usr/bin/kafka-console-consumer --zookeeper hostname:2181 --topic test
~~~

- Step4: type in some message in producer. If consumer can receive the messages sent from producer, then kafka is working fine. Otherwise please check the configuration and logs to identify the root cause of issues.



### Logstash

#### Installation

You can follow [logstash online doc](https://www.elastic.co/downloads/logstash) to download and install logstash on your machine:

Or you can install it through ``yum`` if you are using centos:

- download and install the public signing key:

~~~
rpm --import  https://packages.elastic.co/GPG-KEY-elasticsearch
~~~

- Add the following lines in ``/etc/yum.repos.d/`` directory in a file with a ``.repo`` suffix, for example ``logstash.repo``.

~~~
[logstash-2.3]
name=Logstash repository for 2.3.x packages
baseurl=https://packages.elastic.co/logstash/2.3/centos
gpgcheck=1
gpgkey=https://packages.elastic.co/GPG-KEY-elasticsearch
enabled=1
~~~

- Then install it using ``yum``: 

~~~
yum install logstash
~~~

#### Create conf file

Follow [Apache Eagle online documentation](https://github.com/apache/incubator-eagle/blob/branch-0.4/eagle-assembly/src/main/docs/logstash-kafka-conf.md) to create logstash configuration file for Apache Eagle.

#### Start logstash

~~~
bin/logstash -f conf/first-pipeline.conf
~~~

#### Verification

Open a terminal and start a kafka consumer to see if it can receive the messages sent by logstash, if there is no message, double check the configuration parameters in conf file. Otherwise logstash is all set.

### Apache Storm
As Apache Storm is not in Cloudera’s stack, we need to install Storm manually.

#### Installation

Download Apache Storm from [here](http://storm.apache.org/downloads.html), the version you choose should be ``0.10.x`` or ``0.9.x`` release.
Then follow [Apache Storm online doc](http://storm.apache.org/releases/0.10.0/Setting-up-a-Storm-cluster.html)) to install Apache Storm on your cluster.

In ``/etc/profile``, add this: 

~~~
export PATH=$PATH:/opt/apache-storm-0.10.1/bin/
~~~

save the profile and then type:

~~~
source /etc/profile 
~~~

to make it work.

#### Configuration

In ``storm/conf/storm.yaml``, change the hostname to your own host.

#### Start Apache Storm

In Termial, type:

~~~
$: storm nimbus
$: storm supervisor
$: storm UI
~~~

#### Verification

Open storm UI in your browser, default URL is : ``http://hostname:8080/index.html``.

### Apache Eagle

To download and install Apache Eagle, please refer to  [Get Started with Sandbox.](http://eagle.incubator.apache.org/docs/quick-start.html) .

One thing need to mention is: in ``“/bin/eagle-topology.sh”``, line 102:

~~~			
			storm_ui=http://localhost:8080
~~~

If you are not using the default port number, change this to your own Storm UI url.

I know it takes time to finish these configuration, but now it is time to have fun! 
Just try ``HDFS Data Activity Monitoring`` with ``Demo`` listed in [HDFS Data Activity Monitoring.](http://eagle.incubator.apache.org/docs/hdfs-data-activity-monitoring.html)

