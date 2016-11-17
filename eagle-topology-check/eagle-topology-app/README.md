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

Development in IDE

## 1. Start eagle-server
In IDE, configure the following main class and program arguments

org.apache.eagle.server.ServerMain server src/main/resources/configuration.yml

## 2. Start alert engine

### 2.1 Create new site

http://localhost:9090/rest/sites POST
```
{
"siteId" : "testsite",
"siteName" :"testsite",
"description" : "test description",
"context" : {}
}
```

###n2.2 Create logic alert engine topology

http://localhost:9090/rest/metadata/topologies POST
```
{
   "name": "alertUnitTopology_1",
   "numOfSpout": 1,
   "numOfAlertBolt": 10,
   "numOfGroupBolt": 4,
   "spoutId": "alertEngineSpout",
   "groupNodeIds": [
      "streamRouterBolt0",
      "streamRouterBolt1",
      "streamRouterBolt2",
      "streamRouterBolt3"
   ],
   "alertBoltIds": [
      "alertBolt0",
      "alertBolt1",
      "alertBolt2",
      "alertBolt3",
      "alertBolt4",
      "alertBolt5",
      "alertBolt6",
      "alertBolt7",
      "alertBolt8",
      "alertBolt9"
   ],
   "pubBoltId": "alertPublishBolt",
   "spoutParallelism": 1,
   "groupParallelism": 1,
   "alertParallelism": 1
}
```

### 2.3 Install alert engine application
Please reference eagle-core/eagle-alert-parent/eagle-alert-app/src/main/resources/META-INF/providers/org.apache.eagle.alert.app.AlertUnitTopologyAppProvider.xml for
complete configuration.

http://localhost:9090/rest/apps/install POST
```
{
"siteId" : "testsite",
"appType" : "AlertUnitTopologyApp",
"mode" : "LOCAL",
"configuration" : {
  }
}
```

## 3 Start Hdfs audit log monitoring application

### 3.1 Install TopologyHealthCheck app

http://localhost:9090/rest/apps/install POST
```
{
"siteId" : "sandbox",
"appType" : "TopologyHealthCheckApplication",
"mode" : "LOCAL",
"configuration" : {
  "dataSourceConfig.topic" :"topology_health_check"}
}

## 4 Check
### 4.1 Check if alert data source is created
http://localhost:9090/rest/metadata/datasources GET

### 4.2 Check if alert stream is creatd
http://localhost:9090/rest/metadata/streams GET

## 5 Create alert policy and verify alert
### 5.1 create one policy

http://localhost:9090/rest/metadata/policies POST
```
{
   "name": "hdfsPolicy",
   "description": "hdfsPolicy",
   "inputStreams": [
      "topology_health_check_stream"
   ],
   "outputStreams": [
      "topology_health_check_stream_out"
   ],
   "definition": {
      "type": "siddhi",
      "value": "from TOPOLOGY_HEALTH_CHECK_STREAM_SANDBOX[status=='live'] select * insert into topology_health_check_stream_out"
   },
   "partitionSpec": [
      {
         "streamId": "topology_health_check_stream",
         "type": "GROUPBY",
         "columns" : [
            "host"
         ]
      }
   ],
   "parallelismHint": 2
}
```

### 5.2 Create alert publishment
```
{
	"name":"topology_health_check_stream_out",
	"type":"org.apache.eagle.alert.engine.publisher.impl.AlertEmailPublisher",
	"policyIds": [
		"hdfsPolicy"
	],
	"properties": {
	  "subject":"alert when user is hadoop",
	  "template":"",
	  "sender": "yupu@ebay.com",
	  "recipients": "jianzhchen@ebay.com",
	  "mail.smtp.host":"atom.corp.ebay.com",
	  "connection": "plaintext",
	  "mail.smtp.port": "25"
	},
	"dedupIntervalMin" : "PT1M",
	"serializer" : "org.apache.eagle.alert.engine.publisher.impl.StringEventSerializer"
}
```

### 5.3 Send message and verify alert
./kafka-console-producer.sh --topic topology_health_check --broker-list sandbox.hortonworks.com:6667