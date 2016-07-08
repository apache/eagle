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
-->

## Prerequisites

* [Apache Maven](https://maven.apache.org/)
* [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)

## Documentation


## Build

    mvn install
    
## Setup
The alert engine have three dependency module: Coordinator Service, Metadata Service, and engine runtime(storm topologies).

####0. Dependencies
> Alert engine need kafka as data source, ZK as coordination. Check alert-devtools/bin to start zk and kafka through start-zk-kafka.sh.

####1. Start metadata service & coordinator service
> For local dev, project alert-service packaging as a war, and enabled mvn jetty:run to run it. By default, metadata runs on localhost:8080/rest

> For deployment, after mvn install, a war is avaialble in alert-service/target

####2. Start engine runtime.
> The engine are the topologies that runs in any storm (local or remote) with configuration to connect to the ZK and metadata service. The alert engine runtime main as in UnitTopologyMain.java. The started storm bolt should have the same name(and numbers config) described in alert-metadata. Example of the configuration is /alert-engine/src/main/resources/application.conf 

See below detailed steps.


## Run
* pre-requisites
  * zookeeper
  * storm
  * kafka
  * tomcat
  * mongdb

* Run Metadata service & Coordinator service
    1. copy alert-service/target/alert-service-0.0.1-SNAPSHOT.war into tomcat webapps/alert.war
    2. check config under webapps/alert/WEB-INF/classes/application.conf
    ```json
    {
	"datastore": {
		"metadataDao": "org.apache.eagle.alert.metadata.impl.MongoMetadataDaoImpl",
		"connection": "localhost:27017"
	},
	"coordinator" : {
		"policiesPerBolt" : 5,
		"boltParallelism" : 5,
		"policyDefaultParallelism" : 5,
		"boltLoadUpbound": 0.8,
		"topologyLoadUpbound" : 0.8,
		"numOfAlertBoltsPerTopology" : 5,
		"zkConfig" : {
			"zkQuorum" : "localhost:2181",
			"zkRoot" : "/alert",
			"zkSessionTimeoutMs" : 10000,
			"connectionTimeoutMs" : 10000,
			"zkRetryTimes" : 3,
			"zkRetryInterval" : 3000
		},
		"metadataService" : {
			"host" : "localhost",
			"port" : 8080,
			"context" : "/alert/api"
		},
		"metadataDynamicCheck" : {
			"initDelayMillis" : 1000,
			"delayMillis" : 30000
		}
	}
     }
    ```
    
    3. start tomcat
    
* Run UnitTopologyMain
    1. copy alert-assembly/target/alert-engine-0.0.1-SNAPSHOT-alert-assembly.jar to somewhere close to your storm installation
    2. check config application.conf
   ```json
  {
  "topology" : {
    "name" : "alertUnitTopology_1",
    "numOfTotalWorkers" : 2,
    "numOfSpoutTasks" : 1,
    "numOfRouterBolts" : 4,
    "numOfAlertBolts" : 10,
    "numOfPublishTasks" : 1,
    "messageTimeoutSecs": 3600,
    "localMode" : "true"
  },
  "spout" : {
    "kafkaBrokerZkQuorum": "localhost:2181",
    "kafkaBrokerZkBasePath": "/brokers",
    "stormKafkaUseSameZkQuorumWithKafkaBroker": true,
    "stormKafkaTransactionZkQuorum": "",
    "stormKafkaTransactionZkPath": "/consumers",
    "stormKafkaEagleConsumer": "eagle_consumer",
    "stormKafkaStateUpdateIntervalMs": 2000,
    "stormKafkaFetchSizeBytes": 1048586,
  },
  "zkConfig" : {
    "zkQuorum" : "localhost:2181",
    "zkRoot" : "/alert",
    "zkSessionTimeoutMs" : 10000,
    "connectionTimeoutMs" : 10000,
    "zkRetryTimes" : 3,
    "zkRetryInterval" : 3000
  },
  "dynamicConfigSource" : {
    "initDelayMillis": 3000,
    "delayMillis" : 10000
  },
  "metadataService": {
    "host" : "localhost",
    "context" : "/alert/rest",
    "port" : 8080
  },
  "coordinatorService": {
    "host": "localhost",
    "port": 8080,
    "context" : "/alert/rest"
  },
  "metric": {
    "sink": {
      "stdout": {}
    }
  }
}
```
  Note: please make sure the above configuration is used by storm instead of the configuration within fat jar
  3. start storm
     storm jar alert-engine-0.0.1-SNAPSHOT-alert-assembly.jar org.apache.eagle.alert.engine.UnitTopologyMain

## Support

