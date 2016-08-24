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

package org.apache.eagle.jpm.mr.running;


import org.apache.eagle.jpm.mr.running.config.MRRunningConfigManager;
import org.apache.eagle.jpm.mr.running.storm.MRRunningJobFetchSpout;
import org.apache.eagle.jpm.mr.running.storm.MRRunningJobParseBolt;
import org.apache.eagle.jpm.util.Constants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import java.util.List;

public class MRRunningJobMain {
    public static void main(String[] args) {

        try {
            //1. trigger init conf
            MRRunningConfigManager mrRunningConfigManager = MRRunningConfigManager.getInstance(args);

            List<String> confKeyKeys = mrRunningConfigManager.getConfig().getStringList("MRConfigureKeys.jobConfigKey");
            confKeyKeys.add(Constants.JobConfiguration.CASCADING_JOB);
            confKeyKeys.add(Constants.JobConfiguration.HIVE_JOB);
            confKeyKeys.add(Constants.JobConfiguration.PIG_JOB);
            confKeyKeys.add(Constants.JobConfiguration.SCOOBI_JOB);
            confKeyKeys.add(0, mrRunningConfigManager.getConfig().getString("MRConfigureKeys.jobNameKey"));

            //2. init topology
            TopologyBuilder topologyBuilder = new TopologyBuilder();
            String topologyName = mrRunningConfigManager.getConfig().getString("envContextConfig.topologyName");
            String spoutName = "mrRunningJobFetchSpout";
            String boltName = "mrRunningJobParseBolt";
            int parallelism = mrRunningConfigManager.getConfig().getInt("envContextConfig.parallelismConfig." + spoutName);
            int tasks = mrRunningConfigManager.getConfig().getInt("envContextConfig.tasks." + spoutName);
            if (parallelism > tasks) {
                parallelism = tasks;
            }
            topologyBuilder.setSpout(
                    spoutName,
                    new MRRunningJobFetchSpout(
                            mrRunningConfigManager.getJobExtractorConfig(),
                            mrRunningConfigManager.getEndpointConfig(),
                            mrRunningConfigManager.getZkStateConfig()),
                    parallelism
            ).setNumTasks(tasks);

            parallelism = mrRunningConfigManager.getConfig().getInt("envContextConfig.parallelismConfig." + boltName);
            tasks = mrRunningConfigManager.getConfig().getInt("envContextConfig.tasks." + boltName);
            if (parallelism > tasks) {
                parallelism = tasks;
            }
            topologyBuilder.setBolt(boltName,
                    new MRRunningJobParseBolt(
                            mrRunningConfigManager.getEagleServiceConfig(),
                            mrRunningConfigManager.getEndpointConfig(),
                            mrRunningConfigManager.getJobExtractorConfig(),
                            mrRunningConfigManager.getZkStateConfig(),
                            confKeyKeys),
                    parallelism).setNumTasks(tasks).fieldsGrouping(spoutName, new Fields("appId"));

            backtype.storm.Config config = new backtype.storm.Config();
            config.setNumWorkers(mrRunningConfigManager.getConfig().getInt("envContextConfig.workers"));
            config.put(Config.TOPOLOGY_DEBUG, true);
            if (!mrRunningConfigManager.getEnv().equals("local")) {
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
