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


# Hadoop JMX Collector to Kafka

Python script to collect JMX metrics for any Hadoop component, and send it to Kafka topic

### How to use it

  1. Edit the configuration file config.json. For example:
            ```
            {
             "env": {
              "site": "sandbox"
             },
             "inputs": [
                {
                  "component": "namenode",
                  "host": "127.0.0.1",
                  "port": "50070",
                  "https": false,
                  "kafka_topic": "nn_jmx_metric_sandbox"
                },
                {
                  "component": "resourcemanager",
                  "host": "127.0.0.1",
                  "port": "8088",
                  "https": false,
                  "kafka_topic": "rm_jmx_metric_sandbox"
                },
                {
                  "component": "datanode",
                  "host": "127.0.0.1",
                  "port": "50075",
                  "https": false,
                  "kafka_topic": "dn_jmx_metric_sandbox"
                }
             ],
             "filter": {
              "monitoring.group.selected": ["hadoop", "java.lang"]
             },
             "output": {
             }
            }
            ```
  2. Run the scripts

        ```
        python hadoop_jmx_kafka.py
        ```

### Editing config.json

* inputs

  "port" defines the hadoop service port, such as 50070 => "namenode", 16010 => "hbasemaster".
  Like the example above, you can specify multiple hadoop components to collect

  "https" is whether or not you want to use SSL protocol in your connection to the host+port

  "kafka_topic" is the kafka topic that you want to populate with the jmx data from the respective component

* filter

  "monitoring.group.selected" can filter out beans which we care about.

* output

  You can specify the kafka broker list
        ```
        "output": {
          "kafka": {
            "brokerList": [ "localhost:9092"]
          }
        }
        ```

  To check that the a desired kafka topic is being populated:
    ```
    kafka-console-consumer --zookeeper localhost:2181 --topic nn_jmx_metric_sandbox
    ```
