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

Follow below steps to get Hdfs authorization logs monitoring running

### Step 1: logstash: fetch log file to Kafka
#### 1.1 create topic for raw log
/usr/hdp/2.2.4.2-2/kafka/bin/kafka-topics.sh --create --topic sandbox_hbase_audit_log --partitions 2 --replication-factor 1 --zookeeper localhost:2181
#### 1.2 consume raw log
/usr/hdp/2.2.4.2-2/kafka/bin/kafka-console-consumer.sh --topic sandbox_hbase_audit_log --zookeeper sandbox.hortonworks.com:2181
#### 1.3 create logstash config file: hbase-auditlog.conf
download logstash 2.3.x
~~~
    input {
        file {
            type => "hbase-auditlog"
            path => "/var/log/hbase/SecurityAuth.audit"
            start_position => end
            sincedb_path => "/var/log/logstash/hbase-auditlog-sincedb"
        }
    }

    output {
         if [type] == "hbase-auditlog" {
              kafka {
                  codec => plain {
                      format => "%{message}"
                  }
                  bootstrap_servers => "sandbox.hortonworks.com:6667"
                  topic_id => "sandbox_hbase_audit_log"
                  acks => "0"
                  timeout_ms => 10000
                  retries => 3
                  retry_backoff_ms => 100
                  batch_size => 16384
                  send_buffer_bytes => 131072
                  client_id => "hbase-auditlog"
              }
              # stdout { codec => rubydebug }
          }
    }

~~~
#### 1.4 run logstash
     bin/logstash -f hbase-auditlog.conf

### Step 2: hbase-auditlog topology: parse log and persist to Kafka
#### 2.1 create topic for normalized log
/usr/hdp/2.2.4.2-2/kafka/bin/kafka-topics.sh --create --topic sandbox_hbase_audit_log_parsed --partitions 2 --replication-factor 1 --zookeeper localhost:2181
#### 2.2 consume normalized log
/usr/hdp/2.2.4.2-2/kafka/bin/kafka-console-consumer.sh --topic sandbox_hbase_audit_log_parsed --zookeeper sandbox.hortonworks.com:2181
#### 2.3 run eagle webservice
find eagle-webservice project, run it
#### 2.4 run eagle-security-hdfs-authlog topology
find org.apache.eagle.security.hbase.HbaseAuditLogMonitoringMain, run it

### Step 3: alert engine: consume parsed log
#### 3.1 run alert engine
find org.apache.eagle.alert.engine.UnitTopologyMain, run it


### test to produce message
##### produce raw log
/usr/hdp/2.2.4.2-2/kafka/bin/kafka-console-producer.sh --topic sandbox_hbase_audit_log --broker-list sandbox.hortonworks.com:6667

##### produce parsed log
/usr/hdp/2.2.4.2-2/kafka/bin/kafka-console-producer.sh --topic sandbox_hbase_audit_log_parsed --broker-list sandbox.hortonworks.com:6667


