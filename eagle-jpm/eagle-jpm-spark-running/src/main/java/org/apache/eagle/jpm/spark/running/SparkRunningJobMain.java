/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.spark.running;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.eagle.jpm.spark.running.common.SparkRunningConfigManager;
import org.apache.eagle.jpm.spark.running.storm.SparkRunningJobFetchSpout;
import org.apache.eagle.jpm.spark.running.storm.SparkRunningJobParseBolt;

public class SparkRunningJobMain {
    public static void main(String[] args) {
        try {
            //1. trigger init conf
            SparkRunningConfigManager sparkRunningConfigManager = SparkRunningConfigManager.getInstance(args);

            //2. init topology
            TopologyBuilder topologyBuilder = new TopologyBuilder();
            String topologyName = sparkRunningConfigManager.getConfig().getString("envContextConfig.topologyName");
            String spoutName = "sparkRunningJobFetchSpout";
            String boltName = "sparkRunningJobParseBolt";
            int parallelism = sparkRunningConfigManager.getConfig().getInt("envContextConfig.parallelismConfig." + spoutName);
            int tasks = sparkRunningConfigManager.getConfig().getInt("envContextConfig.tasks." + spoutName);
            if (parallelism > tasks) {
                parallelism = tasks;
            }
            topologyBuilder.setSpout(
                    spoutName,
                    new SparkRunningJobFetchSpout(
                            sparkRunningConfigManager.getJobExtractorConfig(),
                            sparkRunningConfigManager.getEndpointConfig()),
                    parallelism
            ).setNumTasks(tasks);

            parallelism = sparkRunningConfigManager.getConfig().getInt("envContextConfig.parallelismConfig." + boltName);
            tasks = sparkRunningConfigManager.getConfig().getInt("envContextConfig.tasks." + boltName);
            if (parallelism > tasks) {
                parallelism = tasks;
            }
            topologyBuilder.setBolt(boltName,
                    new SparkRunningJobParseBolt(
                            sparkRunningConfigManager.getEagleServiceConfig(),
                            sparkRunningConfigManager.getEndpointConfig(),
                            sparkRunningConfigManager.getJobExtractorConfig()),
                    parallelism).setNumTasks(tasks).fieldsGrouping(spoutName, new Fields("appId"));

            backtype.storm.Config config = new backtype.storm.Config();
            config.setNumWorkers(sparkRunningConfigManager.getConfig().getInt("envContextConfig.workers"));
            config.put(Config.TOPOLOGY_DEBUG, true);
            if (!sparkRunningConfigManager.getEnv().equals("local")) {
                //cluster mode
                //parse conf here
                StormSubmitter.submitTopology(topologyName, config, topologyBuilder.createTopology());
            } else {
                //local mode
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topologyName, config, topologyBuilder.createTopology());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
