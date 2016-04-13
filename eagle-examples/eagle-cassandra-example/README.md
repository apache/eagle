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

Cassandra Security Monitoring Example
=====================================

How to Start 
------------
1. Quickly start all services including zookeeper, kafka, webservice and topology with single script

        bin/quick-start.sh
    
2. Visit eagle web at [http://localhost:9099/eagle-service](http://localhost:9099/eagle-service/ui/#/common/policyList)

3. Send sample data in another terminal with:

        bin/send-sample-querylog.sh

4. Visit alert page at [http://localhost:9099/eagle-service/ui/#/common/alertList](http://localhost:9099/eagle-service/ui/#/common/alertList)

FAQ
---
#### 1. UNKNOWN_TOPIC_OR_PARTITION

> storm.kafka.FailedFetchException: Error fetching data from [Partition{host=localhost:6667, partition=2}] for topic [cassandra_querylog_local]: [UNKNOWN_TOPIC_OR_PARTITION]
     
__Solution__: Execute `eagle-dev/clean-all.sh` and retry from first step.
