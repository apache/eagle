---
layout: doc
title:  "How to stream hdfs log data into Kafka"
permalink: /docs/import-hdfs-auditLog.html
---

As Apache Eagle consumes the data via Kafka[^KAFKA] topics in some topologies, such as HDFS audit log. To enable the full function of monitoring, a user needs to stream its data into a Kafka topic.

There are two ways to do that. The first one is **Logstash**, which naturally supports Kafka as the output plugin; the second one is to
install a **namenode log4j Kafka appender**.

### Logstash-kafka

* **Step 1**: Create a Kafka topic as the streaming input.

    Here is an sample Kafka command to create topic 'sandbox_hdfs_audit_log'

      cd <kafka-home>
      bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sandbox_hdfs_audit_log

* **Step 2**: Install Logstash-kafka plugin

    * For Logstash 1.5.x, logstash-kafka has been intergrated into [logstash-input-kafka](https://github.com/logstash-plugins/logstash-input-kafka) and [logstash-output-kafka](https://github.com/logstash-plugins/logstash-output-kafka),
    and released with the 1.5 version of Logstash. So you can directly use it.

    * For Logstash 1.4.x, a user should install [logstash-kafka](https://github.com/joekiller/logstash-kafka) firstly. Notice that this version **does not support partition\_key\_format**.

* **Step 3**: Create a Logstash configuration file under ${LOGSTASH_HOME}/conf. Here is a sample.

        input {
            file {
                type => "hdp-nn-audit"
                path => "/path/to/audit.log"
                start_position => end
                sincedb_path => "/var/log/logstash/"
             }
        }

        filter{
            if [type] == "hdp-nn-audit" {
        	   grok {
        	       match => ["message", "ugi=(?<user>([\w\d\-]+))@|ugi=(?<user>([\w\d\-]+))/[\w\d\-.]+@|ugi=(?<user>([\w\d.\-_]+))[\s(]+"]
        	   }
            }
        }

        output {
            if [type] == "hdp-nn-audit" {
                kafka {
                    codec => plain {
                        format => "%{message}"
                    }
                    broker_list => "localhost:9092"
                    topic_id => "sandbox_hdfs_audit_log"
                    request_required_acks => 0
                    request_timeout_ms => 10000
                    producer_type => "async"
                    message_send_max_retries => 3
                    retry_backoff_ms => 100
                    queue_buffering_max_ms => 5000
                    queue_enqueue_timeout_ms => 5000
                    batch_num_messages => 200
                    send_buffer_bytes => 102400
                    client_id => "hdp-nn-audit"
                    partition_key_format => "%{user}"
                }
                # stdout { codec => rubydebug }
            }
        }

* **Step 4**: Start Logstash

      bin/logstash -f conf/sample.conf

* **Step 5**: Check whether logs are flowing into the kafka topic specified by `topic_id`

### Log4j Kafka Appender

> Notice that if you use Ambari[^AMBARI], such as in sandbox, you **must** follow below steps via Ambari UI. In addition, restarting namenode is required.

* **Step 1**: Create a Kafka topic. Here is a example Kafka command for creating topic "sandbox_hdfs_audit_log"

      cd <kafka-home>
      bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sandbox_hdfs_audit_log

* **Step 2**: Configure $HADOOP_CONF_DIR/log4j.properties, and add a log4j appender "KAFKA_HDFS_AUDIT" to hdfs audit logging

      log4j.appender.KAFKA_HDFS_AUDIT=org.apache.eagle.log4j.kafka.KafkaLog4jAppender
      log4j.appender.KAFKA_HDFS_AUDIT.Topic=sandbox_hdfs_audit_log
      log4j.appender.KAFKA_HDFS_AUDIT.BrokerList=sandbox.hortonworks.com:6667
      log4j.appender.KAFKA_HDFS_AUDIT.KeyClass=org.apache.eagle.log4j.kafka.hadoop.AuditLogKeyer
      log4j.appender.KAFKA_HDFS_AUDIT.Layout=org.apache.log4j.PatternLayout
      log4j.appender.KAFKA_HDFS_AUDIT.Layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n
      log4j.appender.KAFKA_HDFS_AUDIT.ProducerType=async
      #log4j.appender.KAFKA_HDFS_AUDIT.BatchSize=1
      #log4j.appender.KAFKA_HDFS_AUDIT.QueueSize=1

    ![HDFS LOG4J Configuration](/images/docs/hdfs-log4j-conf.png "hdfslog4jconf")

* **Step 3**: Edit $HADOOP_CONF_DIR/hadoop-env.sh, and add the reference to KAFKA_HDFS_AUDIT to HADOOP_NAMENODE_OPTS.

      -Dhdfs.audit.logger=INFO,DRFAAUDIT,KAFKA_HDFS_AUDIT

    ![HDFS Environment Configuration](/images/docs/hdfs-env-conf.png "hdfsenvconf")

* **Step 4**: Edit $HADOOP_CONF_DIR/hadoop-env.sh, and append the following command to it.

      export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/path/to/eagle/lib/log4jkafka/lib/*

    ![HDFS Environment Configuration](/images/docs/hdfs-env-conf2.png "hdfsenvconf2")

* **Step 5**: save the changes and restart the namenode.

* **Step 6**: Check whether logs are flowing into Topic sandbox_hdfs_audit_log

      $ /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic sandbox_hdfs_audit_log




---

#### *Footnotes*

[^AMBARI]:*all mentions of "ambari" on this page represent Apache Ambari.*
[^KAFKA]:*All mentions of "kafka" on this page represent Apache Kafka.*
