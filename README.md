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

# Apache Eagle

>  Secure Hadoop Data in Real Time

## Overview

Apache Eagle is an open source analytics solution for identifying security and performance issues instantly on big data platforms e.g. Apache Hadoop, Apache Spark, NoSQL etc. It analyzes data activities, yarn applications, jmx metrics, and daemon logs etc., provides state-of-the-art alert engine to identify security breach, performance issues and shows insights.

For more details, please visit [https://eagle.incubator.apache.org](https://eagle.incubator.apache.org)

## Status

### Branches
| name | status | version | description |
| :---:| :---: | :---: | :--- |
| **master** | [![Build Status](https://builds.apache.org/buildStatus/icon?job=incubator-eagle-main)](https://builds.apache.org/job/incubator-eagle-main/) | 0.4.0-incubating | stable code base aligned with latest release |
| **develop** | [![Build Status](https://builds.apache.org/buildStatus/icon?job=incubator-eagle-develop)](https://builds.apache.org/job/incubator-eagle-develop/) | 0.5.0-incubating-SNAPSHOT | code base for continuous development |

### Latest Release

| release date | version | release notes | artifacts | md5 checksum | sha1 checksum |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| 2016/07/21 | [[0.4.0-incubating]](https://github.com/apache/incubator-eagle/releases/tag/v0.4.0-incubating) | [[Release Notes]](https://git-wip-us.apache.org/repos/asf?p=incubator-eagle.git;a=blob_plain;f=CHANGELOG.txt;hb=refs/tags/v0.4.0-incubating) | [[Artifacts]](http://www.apache.org/dyn/closer.cgi?path=/incubator/eagle/apache-eagle-0.4.0-incubating) | [[MD5]](https://dist.apache.org/repos/dist/release/incubator/eagle/apache-eagle-0.4.0-incubating/apache-eagle-0.4.0-incubating-src.tar.gz.md5) | [[SHA1]](https://dist.apache.org/repos/dist/release/incubator/eagle/apache-eagle-0.4.0-incubating/apache-eagle-0.4.0-incubating-src.tar.gz.sha1) |

[More Release Versions](http://archive.apache.org/dist/incubator/eagle/)

## Documentation
You can find the latest Eagle documentation on [https://eagle.incubator.apache.org](https://eagle.incubator.apache.org/docs). This [README](README.md) file only contains basic setup instructions.

## Build Eagle (Supports JDK-1.8)
Eagle is built using [Apache Maven](https://maven.apache.org/). NPM should be installed (On MAC OS try "brew install node"). To build Eagle, run:
    mvn -DskipTests clean package

Note : As of version 0.5, Eagle is tested on JDK-1.8.

After successfully building, you will find eagle binary tarball under _eagle-assembly/target/_

## Eagle Quick Start in IDE (Intellij)
### prepare
Please have HDP sandbox ready, where you can have zookeeper, hadoop, hbase, hive ready  

### Run Eagle Web Service
Go to project eagle-webservice, run it as web application.
 
### Run Eagle Alert Engine
Find out org.apache.eagle.alert.engine.UnitTopologyMain, run it. 

Note: the config is eagle-core/eagle-alert-parent/eagle-alert/alert-engine/src/main/resources/application.conf


### Run Eagle Ingestion Applications
For example, find out org.apache.eagle.security.securitylog.HdfsAuthLogMonitoringMain, run it.

Note: the config is eagle-security/eagle-security-hdfs-authlog/src/main/resources/application.conf

## Get Help
The fastest way to get response from eagle community is to send email to the mail list [dev@eagle.incubator.apache.org](mailto:dev@eagle.incubator.apache.org),
and remember to subscribe our mail list via [dev-subscribe@eagle.incubator.apache.org](mailto:dev-subscribe@eagle.incubator.apache.org)

## FAQ
[https://cwiki.apache.org/confluence/display/EAG/FAQ](https://cwiki.apache.org/confluence/display/EAG/FAQ)

## License
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0). More details, please refer to [LICENSE](LICENSE) file.
