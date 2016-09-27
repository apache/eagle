/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.jpm.mr.running;

import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.jpm.mr.running.storm.MRRunningJobFetchSpout;
import org.apache.eagle.jpm.mr.running.storm.MRRunningJobParseBolt;
import org.apache.eagle.jpm.util.Constants;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MRRunningJobApplication extends StormApplication {
    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        //1. trigger init conf
        MRRunningJobConfig mrRunningJobConfig = MRRunningJobConfig.getInstance(config);

        String[] confKeyPatternsSplit = mrRunningJobConfig.getConfig().getString("MRConfigureKeys.jobConfigKey").split(",");
        List<String> confKeyKeys = new ArrayList<>(confKeyPatternsSplit.length);
        for (String confKeyPattern : confKeyPatternsSplit) {
            confKeyKeys.add(confKeyPattern.trim());
        }
        confKeyKeys.add(Constants.JobConfiguration.CASCADING_JOB);
        confKeyKeys.add(Constants.JobConfiguration.HIVE_JOB);
        confKeyKeys.add(Constants.JobConfiguration.PIG_JOB);
        confKeyKeys.add(Constants.JobConfiguration.SCOOBI_JOB);
        confKeyKeys.add(0, mrRunningJobConfig.getConfig().getString("MRConfigureKeys.jobNameKey"));

        //2. init topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        String spoutName = "mrRunningJobFetchSpout";
        String boltName = "mrRunningJobParseBolt";
        int parallelism = mrRunningJobConfig.getConfig().getInt("envContextConfig.parallelismConfig." + spoutName);
        int tasks = mrRunningJobConfig.getConfig().getInt("envContextConfig.tasks." + spoutName);
        if (parallelism > tasks) {
            parallelism = tasks;
        }
        topologyBuilder.setSpout(
            spoutName,
            new MRRunningJobFetchSpout(
                mrRunningJobConfig.getJobExtractorConfig(),
                mrRunningJobConfig.getEndpointConfig(),
                mrRunningJobConfig.getZkStateConfig()),
            parallelism
        ).setNumTasks(tasks);

        parallelism = mrRunningJobConfig.getConfig().getInt("envContextConfig.parallelismConfig." + boltName);
        tasks = mrRunningJobConfig.getConfig().getInt("envContextConfig.tasks." + boltName);
        if (parallelism > tasks) {
            parallelism = tasks;
        }
        topologyBuilder.setBolt(boltName,
            new MRRunningJobParseBolt(
                mrRunningJobConfig.getEagleServiceConfig(),
                mrRunningJobConfig.getEndpointConfig(),
                mrRunningJobConfig.getJobExtractorConfig(),
                mrRunningJobConfig.getZkStateConfig(),
                confKeyKeys),
            parallelism).setNumTasks(tasks).fieldsGrouping(spoutName, new Fields("appId"));
        return topologyBuilder.createTopology();
    }
}