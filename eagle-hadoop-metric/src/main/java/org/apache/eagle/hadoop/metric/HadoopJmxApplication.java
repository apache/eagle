/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.hadoop.metric;

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
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSpoutProvider;
import storm.kafka.StringScheme;

/**
 * Since 8/12/16.
 * This application just pass through data from jmx metric
 * For persistence or alert purpose, it is not necessary to start application
 * But keep this application in case of future business process
 *
 * Note: this application should be run as multiple instances based on different topic for data source
 */
public class HadoopJmxApplication extends StormApplication {
    public final static String SPOUT_TASK_NUM = "topology.numOfSpoutTasks";
    public final static String PARSER_TASK_NUM = "topology.numOfParserTasks";
    public final static String SINK_TASK_NUM = "topology.numOfSinkTasks";

    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        int numOfSpoutTasks = config.getInt(SPOUT_TASK_NUM);
        int numOfParserTasks = config.getInt(PARSER_TASK_NUM);
        int numOfSinkTasks = config.getInt(SINK_TASK_NUM);

        TopologyBuilder builder = new TopologyBuilder();

        KafkaSpoutProvider provider = new KafkaSpoutProvider();
        IRichSpout spout = provider.getSpout(config);
        builder.setSpout("ingest", spout, numOfSpoutTasks);

        JsonParserBolt bolt = new JsonParserBolt();
        BoltDeclarer boltDeclarer = builder.setBolt("parserBolt", bolt, numOfParserTasks);
        boltDeclarer.fieldsGrouping("ingest", new Fields(StringScheme.STRING_SCHEME_KEY));

        StormStreamSink sinkBolt = environment.getStreamSink("hadoop_jmx_stream",config);
        BoltDeclarer kafkaBoltDeclarer = builder.setBolt("kafkaSink", sinkBolt, numOfSinkTasks);
        kafkaBoltDeclarer.fieldsGrouping("parserBolt", new Fields("f1"));

        return builder.createTopology();
    }

    public static void main(String[] args){
        Config config = ConfigFactory.load();
        HadoopJmxApplication app = new HadoopJmxApplication();
        app.run(config);
    }
}
