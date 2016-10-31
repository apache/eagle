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

package org.apache.eagle.jpm.spark.history;

import org.apache.eagle.app.StormApplication;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.jpm.spark.history.storm.SparkHistoryJobParseBolt;
import org.apache.eagle.jpm.spark.history.storm.SparkHistoryJobSpout;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import com.typesafe.config.Config;

public class SparkHistoryJobApp extends StormApplication {
    @Override
    public StormTopology execute(Config config, StormEnvironment environment) {
        // 1. Init conf
        SparkHistoryJobAppConfig sparkHistoryJobAppConfig = SparkHistoryJobAppConfig.getInstance(config);

        final String jobFetchSpoutName = SparkHistoryJobAppConfig.SPARK_HISTORY_JOB_FETCH_SPOUT_NAME;
        final String jobParseBoltName = SparkHistoryJobAppConfig.SPARK_HISTORY_JOB_PARSE_BOLT_NAME;

        // 2. Config topology.
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout(
                jobFetchSpoutName,
                new SparkHistoryJobSpout(sparkHistoryJobAppConfig), sparkHistoryJobAppConfig.stormConfig.numOfSpoutExecutors
        ).setNumTasks(sparkHistoryJobAppConfig.stormConfig.numOfSpoutTasks);

        topologyBuilder.setBolt(
                jobParseBoltName,
                new SparkHistoryJobParseBolt(sparkHistoryJobAppConfig),
                sparkHistoryJobAppConfig.stormConfig.numOfParserBoltExecutors
        ).setNumTasks(sparkHistoryJobAppConfig.stormConfig.numOfParserBoltTasks).shuffleGrouping(jobFetchSpoutName);

        return topologyBuilder.createTopology();
    }
}
