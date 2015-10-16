Install kafka log4j into namenode
=================================

Installation
------------
1. **Add eagle kafka log4j jars into hadoop namenode classpath**: append `$EAGLE_HOME/lib/log4jkafka/lib/*` into value of `$HADOOP_CLASSPATH` in `$HADOOP_CONF_DIR/hadoop-env.sh`
2. **Change log4j configuration aside namenode**: Add **one** of following types of log4j configuration ("xml" or "properties") into `$HADOOP_CONF_DIR` and modify kafka related configuration like broker hosts, topic, queue size, batch size correspondingly.
  * **log4j.xml**: refer to [log4j-template.xml](https://github.xyz.com/eagle/eagle/blob/master/eagle-external/eagle-log4jkafka/src/main/resources/log4j-template.xml)
 
          <appender class=".eagle.kafka.producer.JKafkaLog4jAppender" name="KAFKA_HDFS_AUDIT">
              <param value="sandbox.hortonworks.com:6667" name="BrokerList"/>
              <param value=".eagle.kafka.hadoop.AuditLogKeyer" name="KeyClass"/>
              <param value="hdfs_audit_log" name="Topic"/>
              <layout class="org.apache.log4j.PatternLayout">
                  <param value="%d{ISO8601} %p %c{2}: %m%n" name="ConversionPattern"/>
              </layout>
          </appender>
          <appender name="ASYNC_KAFKA_HDFS_AUDIT" class="org.apache.log4j.AsyncAppender">
              <param name="BufferSize" value="50"/>
              <param name="Blocking" value="false"/>
              <appender-ref ref="KAFKA_HDFS_AUDIT"/>
          </appender>

  * **log4j.properties**: refer to [log4j-template.properties](https://github.xyz.com/eagle/eagle/blob/master/eagle-external/eagle-log4jkafka/src/main/resources/log4j-template.properties)
 
          hdfs.audit.logger=INFO,console,DRFAAUDIT,KAFKA_HDFS_AUDIT
          log4j.appender.KAFKA_HDFS_AUDIT=eagle.log4j.kafka.KafkaLog4jAppender
          log4j.appender.KAFKA_HDFS_AUDIT.Topic=hdfs_audit_log
          log4j.appender.KAFKA_HDFS_AUDIT.BrokerList=sandbox.hortonworks.com:6667
          log4j.appender.KAFKA_HDFS_AUDIT.KeyClass=eagle.log4j.kafka.hadoop.AuditLogKeyer
          log4j.appender.KAFKA_HDFS_AUDIT.Layout=org.apache.log4j.PatternLayout
          log4j.appender.KAFKA_HDFS_AUDIT.Layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n
          log4j.appender.KAFKA_HDFS_AUDIT.ProducerType=async
          log4j.appender.KAFKA_HDFS_AUDIT.BatchSize=10
          log4j.appender.KAFKA_HDFS_AUDIT.QueueSize=20

3. **Restart name node**: Confirm the configuration is correct and restart name-node.
4. **Validate it works**
  * Check name node is correctly started without log4j related exception.
  * Check whether log is flowing into kafka with kafka-console-consumer.sh

Reference
---------
1. Logj4 1.x configuration: https://logging.apache.org/log4j/1.2/manual.html
2. Logj4 2.x configuration: http://logging.apache.org/log4j/2.x/manual/configuration.html 
3. Kafka producer configuration: https://kafka.apache.org/documentation.html#producerconfigs
4. Hadoop configuration: http://wiki.apache.org/hadoop/HowToConfigure

Q&A
---
> TODO: Please add related questions and answers in here 
