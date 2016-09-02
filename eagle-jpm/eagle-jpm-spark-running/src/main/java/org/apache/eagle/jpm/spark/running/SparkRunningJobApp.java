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
package org.apache.eagle.jpm.spark.running;


import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.jpm.spark.running.storm.SparkRunningJobFetchSpout;
import org.apache.eagle.jpm.spark.running.storm.SparkRunningJobParseBolt;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.typesafe.config.Config;

public class SparkRunningJobApp extends StormApplication {
    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        //1. trigger init conf
        SparkRunningJobAppConfig sparkRunningJobAppConfig = SparkRunningJobAppConfig.getInstance(config);

        //2. init topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        final String spoutName = SparkRunningJobAppConfig.JOB_FETCH_SPOUT_NAME;
        final String boltName = SparkRunningJobAppConfig.JOB_PARSE_BOLT_NAME;
        int parallelism = sparkRunningJobAppConfig.getTopologyConfig().jobFetchSpoutParallelism;
        int tasks = sparkRunningJobAppConfig.getTopologyConfig().jobFetchSpoutTasksNum;
        if (parallelism > tasks) {
            parallelism = tasks;
        }
        topologyBuilder.setSpout(
                spoutName,
                new SparkRunningJobFetchSpout(
                        sparkRunningJobAppConfig.getJobExtractorConfig(),
                        sparkRunningJobAppConfig.getEndpointConfig(),
                        sparkRunningJobAppConfig.getZkStateConfig()),
                parallelism
        ).setNumTasks(tasks);

        parallelism = sparkRunningJobAppConfig.getTopologyConfig().jobParseBoltParallelism;
        tasks = sparkRunningJobAppConfig.getTopologyConfig().jobParseBoltTasksNum;
        if (parallelism > tasks) {
            parallelism = tasks;
        }
        topologyBuilder.setBolt(boltName,
                new SparkRunningJobParseBolt(
                        sparkRunningJobAppConfig.getZkStateConfig(),
                        sparkRunningJobAppConfig.getEagleServiceConfig(),
                        sparkRunningJobAppConfig.getEndpointConfig(),
                        sparkRunningJobAppConfig.getJobExtractorConfig()),
                parallelism).setNumTasks(tasks).fieldsGrouping(spoutName, new Fields("appId"));

        return topologyBuilder.createTopology();
    }
}