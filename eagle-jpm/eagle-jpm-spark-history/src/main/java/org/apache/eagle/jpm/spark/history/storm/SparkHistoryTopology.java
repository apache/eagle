/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.jpm.spark.history.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import org.apache.eagle.jpm.spark.history.config.SparkHistoryCrawlConfig;

public class SparkHistoryTopology {

    private SparkHistoryCrawlConfig SHConfig;

    public SparkHistoryTopology(SparkHistoryCrawlConfig config){
        this.SHConfig = config;
    }

    public TopologyBuilder getBuilder() {
        TopologyBuilder builder = new TopologyBuilder();
        String spoutName = "sparkHistoryJobSpout";
        String boltName = "sparkHistoryJobBolt";
        com.typesafe.config.Config config = this.SHConfig.getConfig();
        builder.setSpout(spoutName,
                new FinishedSparkJobSpout(SHConfig),
                config.getInt("storm.parallelismConfig." + spoutName)
        ).setNumTasks(config.getInt("storm.tasks." + spoutName));

        builder.setBolt(boltName,
                new SparkJobParseBolt(SHConfig),
                config.getInt("storm.parallelismConfig." + boltName)
        ).setNumTasks(config.getInt("storm.tasks." + boltName)).shuffleGrouping(spoutName);
        return builder;
    }


    public static void main(String[] args) {
        try {
            SparkHistoryCrawlConfig crawlConfig = new SparkHistoryCrawlConfig();

            Config conf = new Config();
            conf.setNumWorkers(crawlConfig.stormConfig.workerNo);
            conf.setMessageTimeoutSecs(crawlConfig.stormConfig.timeoutSec);
            //conf.setMaxSpoutPending(crawlConfig.stormConfig.spoutPending);
            //conf.put(Config.TOPOLOGY_DEBUG, true);


            if (crawlConfig.stormConfig.mode.equals("local")) {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(
                        crawlConfig.stormConfig.topologyName,
                        conf,
                        new SparkHistoryTopology(crawlConfig).getBuilder().createTopology());
            } else {
                StormSubmitter.submitTopology(
                        crawlConfig.stormConfig.topologyName,
                        conf,
                        new SparkHistoryTopology(crawlConfig).getBuilder().createTopology());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
