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

package org.apache.eagle.jpm.mr.history;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import org.apache.eagle.jpm.mr.history.common.JHFConfigManager;
import org.apache.eagle.jpm.mr.history.common.JPAConstants;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilter;
import org.apache.eagle.jpm.mr.history.crawler.JobHistoryContentFilterBuilder;
import org.apache.eagle.jpm.mr.history.storm.HistoryJobProgressBolt;
import org.apache.eagle.jpm.mr.history.storm.JobHistorySpout;

import java.util.List;
import java.util.regex.Pattern;

public class MRHistoryJobMain {
    public static void main(String []args) {
        try {
            //1. trigger init conf
            JHFConfigManager jhfConfigManager = JHFConfigManager.getInstance(args);
            com.typesafe.config.Config jhfAppConf = jhfConfigManager.getConfig();

            //2. init JobHistoryContentFilter
            JobHistoryContentFilterBuilder builder = JobHistoryContentFilterBuilder.newBuilder().acceptJobFile().acceptJobConfFile();
            List<String> confKeyPatterns = jhfAppConf.getStringList("MRConfigureKeys");
            confKeyPatterns.add(JPAConstants.JobConfiguration.CASCADING_JOB);
            confKeyPatterns.add(JPAConstants.JobConfiguration.HIVE_JOB);
            confKeyPatterns.add(JPAConstants.JobConfiguration.PIG_JOB);
            confKeyPatterns.add(JPAConstants.JobConfiguration.SCOOBI_JOB);

            for (String key : confKeyPatterns) {
                builder.includeJobKeyPatterns(Pattern.compile(key));
            }
            JobHistoryContentFilter filter = builder.build();

            //3. init topology
            TopologyBuilder topologyBuilder = new TopologyBuilder();
            String topologyName = jhfAppConf.getString("envContextConfig.topologyName");
            if (topologyName == null) {
                topologyName = "mrHistoryJobTopology";
            }
            String spoutName = "mrHistoryJobExecutor";
            String boltName = "updateProcessTime";
            int parallelism = jhfAppConf.getInt("envContextConfig.parallelismConfig." + spoutName);
            int tasks = jhfAppConf.getInt("envContextConfig.tasks." + spoutName);
            if (parallelism > tasks) {
                parallelism = tasks;
            }
            topologyBuilder.setSpout(
                    spoutName,
                    new JobHistorySpout(filter, jhfConfigManager),
                    parallelism
            ).setNumTasks(tasks);
            topologyBuilder.setBolt(boltName, new HistoryJobProgressBolt(spoutName, jhfConfigManager), 1).setNumTasks(1).allGrouping(spoutName);

            Config config = new backtype.storm.Config();
            config.setNumWorkers(jhfAppConf.getInt("envContextConfig.workers"));
            config.put(Config.TOPOLOGY_DEBUG, true);
            if (!jhfConfigManager.getEnv().equals("local")) {
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
