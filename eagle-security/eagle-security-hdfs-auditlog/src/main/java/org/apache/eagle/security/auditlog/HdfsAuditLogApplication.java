/*
 *
 *  *
 *  *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  *  * contributor license agreements.  See the NOTICE file distributed with
 *  *  * this work for additional information regarding copyright ownership.
 *  *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  *  * (the "License"); you may not use this file except in compliance with
 *  *  * the License.  You may obtain a copy of the License at
 *  *  * <p/>
 *  *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *  * <p/>
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package org.apache.eagle.security.auditlog;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.app.messaging.StormStreamSink;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSpoutProvider;
import org.apache.eagle.security.traffic.HadoopLogAccumulatorBolt;

/**
 * Since 8/11/16.
 */
public class HdfsAuditLogApplication extends AbstractHdfsAuditLogApplication {
    @Override
    public BaseRichBolt getParserBolt(Config config) {
        return new HdfsAuditLogParserBolt(config);
    }

    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpoutProvider provider = new KafkaSpoutProvider();
        IRichSpout spout = provider.getSpout(config);

        int numOfSpoutTasks = config.getInt(SPOUT_TASK_NUM);
        int numOfParserTasks = config.getInt(PARSER_TASK_NUM);
        int numOfSensitivityJoinTasks = config.getInt(SENSITIVITY_JOIN_TASK_NUM);
        int numOfIPZoneJoinTasks = config.getInt(IPZONE_JOIN_TASK_NUM);
        int numOfSinkTasks = config.getInt(SINK_TASK_NUM);

        builder.setSpout("ingest", spout, numOfSpoutTasks).setNumTasks(numOfSpoutTasks);

        BaseRichBolt parserBolt = getParserBolt(config);
        BoltDeclarer boltDeclarer = builder.setBolt("parserBolt", parserBolt, numOfParserTasks).setNumTasks(numOfParserTasks).shuffleGrouping("ingest");
        boltDeclarer.shuffleGrouping("ingest");

        HdfsSensitivityDataEnrichBolt sensitivityDataJoinBolt = new HdfsSensitivityDataEnrichBolt(config);
        BoltDeclarer sensitivityDataJoinBoltDeclarer = builder.setBolt("sensitivityJoin", sensitivityDataJoinBolt, numOfSensitivityJoinTasks).setNumTasks(numOfSensitivityJoinTasks);
        // sensitivityDataJoinBoltDeclarer.fieldsGrouping("parserBolt", new Fields("f1"));
        sensitivityDataJoinBoltDeclarer.shuffleGrouping("parserBolt");

        // ------------------------------
        // sensitivityJoin -> ipZoneJoin
        // ------------------------------
        IPZoneDataEnrichBolt ipZoneDataJoinBolt = new IPZoneDataEnrichBolt(config);
        BoltDeclarer ipZoneDataJoinBoltDeclarer = builder.setBolt("ipZoneJoin", ipZoneDataJoinBolt, numOfIPZoneJoinTasks).setNumTasks(numOfIPZoneJoinTasks);
        // ipZoneDataJoinBoltDeclarer.fieldsGrouping("sensitivityJoin", new Fields("user"));
        ipZoneDataJoinBoltDeclarer.shuffleGrouping("sensitivityJoin");

        // ------------------------
        // ipZoneJoin -> kafkaSink
        // ------------------------

        StormStreamSink sinkBolt = environment.getStreamSink("HDFS_AUDIT_LOG_ENRICHED_STREAM", config);
        BoltDeclarer kafkaBoltDeclarer = builder.setBolt("kafkaSink", sinkBolt, numOfSinkTasks).setNumTasks(numOfSinkTasks);
        kafkaBoltDeclarer.shuffleGrouping("ipZoneJoin");

        if (config.hasPath(TRAFFIC_MONITOR_ENABLED) && config.getBoolean(TRAFFIC_MONITOR_ENABLED)) {
            builder.setSpout("trafficSpout", environment.getStreamSource("HADOOP_JMX_RESOURCE_STREAM", config), 1)
                    .setNumTasks(1);

            builder.setBolt("trafficParserBolt", new TrafficParserBolt(config), 1)
                    .setNumTasks(1)
                    .shuffleGrouping("trafficSpout");
            builder.setBolt("trafficSinkBolt", environment.getStreamSink("HDFS_AUDIT_LOG_TRAFFIC_STREAM", config), 1)
                    .setNumTasks(1)
                    .shuffleGrouping("trafficParserBolt");
        }

        return builder.createTopology();
    }

    @Override
    public String getSinkStreamName() {
        return "hdfs_audit_log_stream";
    }

    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        HdfsAuditLogApplication app = new HdfsAuditLogApplication();
        app.run(config);
    }
}
