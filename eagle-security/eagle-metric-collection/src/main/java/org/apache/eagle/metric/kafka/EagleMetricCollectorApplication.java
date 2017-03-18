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
package org.apache.eagle.metric.kafka;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Since 8/12/16.
 */
public class EagleMetricCollectorApplication extends StormApplication{
    private static final Logger LOG = LoggerFactory.getLogger(EagleMetricCollectorApplication.class);

    public final static String SPOUT_TASK_NUM = "topology.numOfSpoutTasks";
    public final static String DISTRIBUTION_TASK_NUM = "topology.numOfDistributionTasks";

    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        String deserClsName = config.getString("dataSourceConfig.deserializerClass");
        KafkaSourcedSpoutScheme scheme = new KafkaSourcedSpoutScheme(deserClsName, config);

        TopologyBuilder builder = new TopologyBuilder();
        BaseRichSpout spout1 = new KafkaOffsetSourceSpoutProvider().getSpout(config);
        BaseRichSpout spout2 = KafkaSourcedSpoutProvider.getSpout(config, scheme);

        int numOfSpoutTasks = config.getInt(SPOUT_TASK_NUM);
        int numOfDistributionTasks = config.getInt(DISTRIBUTION_TASK_NUM);

        builder.setSpout("kafkaLogLagChecker", spout1, numOfSpoutTasks);
        builder.setSpout("kafkaMessageFetcher", spout2, numOfSpoutTasks);

        KafkaMessageDistributionBolt bolt = new KafkaMessageDistributionBolt(config);
        BoltDeclarer bolteclarer = builder.setBolt("distributionBolt", bolt, numOfDistributionTasks);
        bolteclarer.fieldsGrouping("kafkaLogLagChecker", new Fields("f1"));
        bolteclarer.fieldsGrouping("kafkaLogLagChecker", new Fields("f1"));
        return builder.createTopology();
    }


    public static void main(String[] args){
        Config config = ConfigFactory.load();
        EagleMetricCollectorApplication app = new EagleMetricCollectorApplication();
        app.run(config);
    }
}
