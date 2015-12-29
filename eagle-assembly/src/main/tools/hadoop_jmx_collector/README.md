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


# Hadoop Jmx Collector

These scripts help to collect Hadoop jmx and evently sent the metrics to stdout or Kafka. Tested with Python 2.7.

### How to use it
  
  1. Edit the configuration file (json file). For example:
         {
           "env": {
            "site": "sandbox"
           },
           "input": {
            "port": "50070",
            "https": false
           },
           "filter": {
            "monitoring.group.selected": ["hadoop"]
           },
           "output": {
           }
        }
     
  2. Run the scripts
  
        # for general use
        python hadoop_jmx_kafka.py > 1.txt
      
### Edit `eagle-collector.conf`

* input

  "port" defines the hadoop service port, such as 50070 => "namenode", 60010 => "hbase master".

* filter
  
  "monitoring.group.selected" can filter out beans which we care about. 

* output 
  
  if we left it empty, then the output is stdout by default. 

        "output": {}
        
  It also supports Kafka as its output. 

        "output": {
          "kafka": {
            "topic": "test_topic",
            "brokerList": [ "sandbox.hortonworks.com:6667"]
          }
        }
      
### Example
       

