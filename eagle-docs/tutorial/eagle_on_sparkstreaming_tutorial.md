<!--
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
-->

### Introduction

Eagle supports spark streaming framework now. Eagle

### Manual for testing

#### Step 1: configuration

To run eagle on spark streaming, some key parameters should assigned in `application.conf` (the same with eagle on storm). Since eagle on spark streaming only supports kafka
as data resource for now, the zookeeper addresses and kafka topic should be configured correctly.

*`dataSourceConfig.zkConnection`

*`dataSourceConfig.transactionZKServers`

*`dataSourceConfig.topic`

Eagle on spark streaming runs in local mode default. To run in cluster mode, configure below parameters in `application.conf`:

*`envContextConfig.mode` = cluster

*`dataSourceConfig.masterUrl`

*`dataSourceConfig.masterPort`

The batch window (default is 1) can be freely defined in `application.conf`

#### Step 2: start up eagle web service

Start Eagle web service.

#### Step 3: start up alert

After finish the configuration file, eagle on spark streaming can start up now. As an example to monitor cassandraQueryLogStream was provided for testing.

Run `TestSparkStreamingWithAlertDSL` in `eagle-core/eagle-data-process/eagle-stream-process-spark/src/test/scala`.






