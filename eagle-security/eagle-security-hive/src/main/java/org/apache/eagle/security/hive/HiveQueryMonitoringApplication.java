/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.security.hive;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.app.sink.StormStreamSink;
import org.apache.eagle.security.hive.jobrunning.HiveJobRunningSourcedStormSpoutProvider;
import org.apache.eagle.security.hive.jobrunning.HiveQueryParserBolt;
import org.apache.eagle.security.hive.jobrunning.JobFilterBolt;
import org.apache.eagle.security.hive.sensitivity.HiveResourceSensitivityDataJoinBolt;

/**
 * Since 8/11/16.
 */
public class HiveQueryMonitoringApplication extends StormApplication {
    public final static String SPOUT_TASK_NUM = "topology.numOfSpoutTasks";
    public final static String FILTER_TASK_NUM = "topology.numOfFilterTasks";
    public final static String PARSER_TASK_NUM = "topology.numOfParserTasks";
    public final static String JOIN_TASK_NUM = "topology.numOfJoinTasks";
    public final static String SINK_TASK_NUM = "topology.numOfSinkTasks";

    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        TopologyBuilder builder = new TopologyBuilder();
        HiveJobRunningSourcedStormSpoutProvider provider = new HiveJobRunningSourcedStormSpoutProvider();
        IRichSpout spout = provider.getSpout(config, config.getInt(SPOUT_TASK_NUM));


        int numOfSpoutTasks = config.getInt(SPOUT_TASK_NUM);
        int numOfFilterTasks = config.getInt(FILTER_TASK_NUM);
        int numOfParserTasks = config.getInt(PARSER_TASK_NUM);
        int numOfJoinTasks = config.getInt(JOIN_TASK_NUM);
        int numOfSinkTasks = config.getInt(SINK_TASK_NUM);

        builder.setSpout("ingest", spout, numOfSpoutTasks);
        JobFilterBolt bolt = new JobFilterBolt();
        BoltDeclarer boltDeclarer = builder.setBolt("filterBolt", bolt, numOfFilterTasks);
        boltDeclarer.fieldsGrouping("ingest", new Fields("jobId"));

        HiveQueryParserBolt parserBolt = new HiveQueryParserBolt();
        BoltDeclarer parserBoltDeclarer = builder.setBolt("parserBolt", parserBolt, numOfParserTasks);
        parserBoltDeclarer.fieldsGrouping("filterBolt", new Fields("user"));

        HiveResourceSensitivityDataJoinBolt joinBolt = new HiveResourceSensitivityDataJoinBolt(config);
        BoltDeclarer joinBoltDeclarer = builder.setBolt("joinBolt", joinBolt, numOfJoinTasks);
        joinBoltDeclarer.fieldsGrouping("parserBolt", new Fields("user"));

        StormStreamSink sinkBolt = environment.getStreamSink("hive_query_stream",config);
        BoltDeclarer kafkaBoltDeclarer = builder.setBolt("kafkaSink", sinkBolt, numOfSinkTasks);
        kafkaBoltDeclarer.fieldsGrouping("joinBolt", new Fields("user"));
        return builder.createTopology();
    }

    public static void main(String[] args){
        Config config = ConfigFactory.load();
        HiveQueryMonitoringApplication app = new HiveQueryMonitoringApplication();
        app.run(config);
    }
}
