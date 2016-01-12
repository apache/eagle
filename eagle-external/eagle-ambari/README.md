<!--
{% comment %}
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
{% endcomment %}
-->

eagle-ambari-plugin
===================

Requirements
------------
1. Ambari `2.2.4` (has problems for higher version)

Installation
------------
1. Download certain version of eagle release package and install under to `/usr/hdp/current/eagle`
2. Execute ambari install

        ./bin/eagle-ambari.sh install

Uninstallation
------------
1. Uninstall services from amabri cluster

        ./bin/eagle-ambari.sh uninstall

Usage
-----
[https://wiki.vip.xyz.com/display/GDI/Eagle+in+Ambari](https://wiki.vip.xyz.com/display/GDI/Eagle+in+Ambari)

Reference
---------
* [Ambari: Create and Add the Service](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=38571133#Overview\(Ambari1.5.0orlater\)-CreateandAddtheService)
* [Quick links for custom services](https://issues.apache.org/jira/browse/AMBARI-11268)
