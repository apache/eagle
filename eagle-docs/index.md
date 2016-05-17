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
<span class="docs-version">v0.3.0-incubating</span>

## Introduction

> Eagle is an Open Source Monitoring solution for Hadoop to instantly detect access to sensitive data, recognize attacks, malicious activities and block access in real time. 

## Key Qualities

* **Real Time**: We understand the importance of timing and acting fast in case of a security breach. So we designed Eagle to make sure the alerts are generated in sub second and stop the anomalous activity if it’s a real threat.
* **Scalability**: At eBay Eagle is deployed on multiple large hadoop clusters with petabytes of data and 800M access events everyday.
* **Ease of Use**: Usability is one of our core design principles. It takes only a few minutes to get started with Eagle sandbox. We have made it easy to get started with good examples and policies can be added with few clicks.
* **User Profiles**: Eagle provides capabilities to create user profiles based on the user behaviour in hadoop. We have out of the box machine learning algorithms that you can leverage to build models with different HDFS features and get alerted on anomalies. 
* **Open Source**: Eagle is built ground up using open source standards and various products from the big data space. We decided to open source Eagle to help the community and looking forward for your feedback, collaboration and support.
* **Extensibility**: Eagle is designed with extensibility in mind. You can integrate Eagle with existing data classification tools and monitoring tools easily.

## Use Cases

Eagle data activity monitoring is currently deployed at eBay for monitoring the data access activities in a 2500 node hadoop cluster with plans of extending it to other hadoop clusters covering 10,000 nodes by end of this year. We have wide range of policies to detect and prevent data loss, data copy to unsecured location, sensitive data access from unauthorized zones etc. The flexibility of creating policies in eagle allows us to expand further and add more complex policies.

Some other typical use cases:

* Monitor data access traffic on Hadoop
* Discover intrusions and security breach
* Discover and prevent sensitive data loss 
* Policy based detection and alerting 
* Anomalous data access detection based on user behaviour 
