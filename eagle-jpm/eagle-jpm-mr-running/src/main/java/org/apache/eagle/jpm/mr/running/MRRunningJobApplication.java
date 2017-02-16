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
import org.apache.eagle.jpm.analyzer.mr.MRJobPerformanceAnalyzer;
import org.apache.eagle.jpm.mr.running.storm.MRRunningAppMetricBolt;
import org.apache.eagle.jpm.mr.running.storm.MRRunningJobFetchSpout;
import org.apache.eagle.jpm.mr.running.storm.MRRunningJobParseBolt;
import org.apache.eagle.jpm.util.Constants;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import com.typesafe.config.Config;
import storm.trident.planner.SpoutNode;

import java.util.ArrayList;
import java.util.List;

import static org.apache.eagle.jpm.mr.running.MRRunningJobConfig.APP_TO_JOB_STREAM;
import static org.apache.eagle.jpm.mr.running.MRRunningJobConfig.APP_TO_METRIC_STREAM;

public class MRRunningJobApplication extends StormApplication {
    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        //1. trigger prepare conf
        MRRunningJobConfig mrRunningJobConfig = MRRunningJobConfig.newInstance(config);

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

        //2. prepare topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        String spoutName = "mrRunningJobFetchSpout";
        String boltName = "mrRunningJobParseBolt";
        int tasks = mrRunningJobConfig.getConfig().getInt("stormConfig." + spoutName + "Tasks");

        topologyBuilder.setSpout(
            spoutName,
            new MRRunningJobFetchSpout(mrRunningJobConfig.getEndpointConfig(),
                    mrRunningJobConfig.getZkStateConfig()),
            tasks
        ).setNumTasks(tasks);

        tasks = mrRunningJobConfig.getConfig().getInt("stormConfig." + boltName + "Tasks");

        topologyBuilder.setBolt(boltName,
            new MRRunningJobParseBolt(
                mrRunningJobConfig.getEagleServiceConfig(),
                mrRunningJobConfig.getEndpointConfig(),
                mrRunningJobConfig.getZkStateConfig(),
                confKeyKeys,
                config),
                tasks).setNumTasks(tasks).fieldsGrouping(spoutName, APP_TO_JOB_STREAM, new Fields("appId"));

        // parse running/accepted app metrics
        topologyBuilder.setBolt("mrRunningJobMetricBolt", new MRRunningAppMetricBolt(mrRunningJobConfig), 1)
                .setNumTasks(1).shuffleGrouping(spoutName, APP_TO_METRIC_STREAM);
        topologyBuilder.setBolt("acceptedAppSink", environment.getStreamSink("ACCEPTED_APP_STREAM", config), 1)
                .setNumTasks(1).shuffleGrouping("mrRunningJobMetricBolt");

        return topologyBuilder.createTopology();
    }
}