/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.apache.eagle.security.oozie.parse;

import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.app.sink.StormStreamSink;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSpoutProvider;
import org.apache.eagle.security.oozie.parse.sensitivity.OozieResourceSensitivityDataJoinBolt;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import storm.kafka.StringScheme;

/**
 * Since 8/12/16.
 */
public class OozieAuditLogApplication extends StormApplication {
    public static final String SPOUT_TASK_NUM = "topology.numOfSpoutTasks";
    public static final String PARSER_TASK_NUM = "topology.numOfParserTasks";
    public static final String JOIN_TASK_NUM = "topology.numOfJoinTasks";
    public static final String SINK_TASK_NUM = "topology.numOfSinkTasks";

    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpoutProvider provider = new KafkaSpoutProvider();
        IRichSpout spout = provider.getSpout(config);

        int numOfSpoutTasks = config.getInt(SPOUT_TASK_NUM);
        int numOfParserTask = config.getInt(PARSER_TASK_NUM);
        int numOfJoinTasks = config.getInt(JOIN_TASK_NUM);

        builder.setSpout("ingest", spout, numOfSpoutTasks);

        OozieAuditLogParserBolt parserBolt = new OozieAuditLogParserBolt();
        BoltDeclarer parserBoltDeclarer = builder.setBolt("parserBolt", parserBolt, numOfParserTask);
        parserBoltDeclarer.fieldsGrouping("ingest", new Fields(StringScheme.STRING_SCHEME_KEY));

        OozieResourceSensitivityDataJoinBolt joinBolt = new OozieResourceSensitivityDataJoinBolt(config);
        BoltDeclarer boltDeclarer = builder.setBolt("joinBolt", joinBolt, numOfJoinTasks);
        boltDeclarer.fieldsGrouping("parserBolt", new Fields("f1"));

        StormStreamSink sinkBolt = environment.getStreamSink("oozie_audit_log_stream", config);
        int numOfSinkTasks = config.getInt(SINK_TASK_NUM);
        BoltDeclarer kafkaBoltDeclarer = builder.setBolt("kafkaSink", sinkBolt, numOfSinkTasks);
        kafkaBoltDeclarer.fieldsGrouping("joinBolt", new Fields("user"));
        return builder.createTopology();
    }

    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        OozieAuditLogApplication app = new OozieAuditLogApplication();
        app.run(config);
    }
}
