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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.eagle.security.securitylog;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSpoutProvider;
import org.apache.eagle.security.topo.TopologySubmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.kafka.bolt.KafkaBolt;

public class HdfsAuthLogMonitoringMain {
    private static Logger LOG = LoggerFactory.getLogger(HdfsAuthLogMonitoringMain.class);
    public final static String SPOUT_TASK_NUM = "topology.numOfSpoutTasks";
    public final static String PARSER_TASK_NUM = "topology.numOfParserTasks";
    public final static String SINK_TASK_NUM = "topology.numOfSinkTasks";

    public static void main(String[] args) throws Exception{
        System.setProperty("config.resource", "/application.conf");
        Config config = ConfigFactory.load();
        KafkaSpoutProvider provider = new KafkaSpoutProvider();
        IRichSpout spout = provider.getSpout(config);

        SecurityLogParserBolt bolt = new SecurityLogParserBolt();
        TopologyBuilder builder = new TopologyBuilder();

        int numOfSpoutTasks = config.getInt(SPOUT_TASK_NUM);
        int numOfParserTasks = config.getInt(PARSER_TASK_NUM);
        int numOfSinkTasks = config.getInt(SINK_TASK_NUM);

        builder.setSpout("ingest", spout, numOfSpoutTasks);
        BoltDeclarer boltDeclarer = builder.setBolt("parserBolt", bolt, numOfParserTasks);
        boltDeclarer.shuffleGrouping("ingest");

        KafkaBolt kafkaBolt = new KafkaBolt();
        BoltDeclarer kafkaBoltDeclarer = builder.setBolt("kafkaSink", kafkaBolt, numOfSinkTasks);
        kafkaBoltDeclarer.shuffleGrouping("parserBolt");

        StormTopology topology = builder.createTopology();

        TopologySubmitter.submit(topology, config);
    }
}