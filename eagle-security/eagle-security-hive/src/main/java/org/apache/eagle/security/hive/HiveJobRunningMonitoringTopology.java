/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.security.hive;


import com.typesafe.config.Config;
import org.apache.eagle.datastream.ExecutionEnvironments;
import org.apache.eagle.datastream.storm.StormExecutionEnvironment;
import org.apache.eagle.security.hive.jobrunning.HiveJobRunningSourcedStormSpoutProvider;
import org.apache.eagle.security.hive.jobrunning.HiveQueryParserExecutor;
import org.apache.eagle.security.hive.jobrunning.JobConfigurationAdaptorExecutor;
import org.apache.eagle.security.hive.sensitivity.HiveResourceSensitivityDataJoinExecutor;
import org.apache.eagle.stream.application.TopologyExecutable;

import java.util.Arrays;

public class HiveJobRunningMonitoringTopology implements TopologyExecutable {
    @Override
    public void submit(String topology, Config config) {
        StormExecutionEnvironment env = ExecutionEnvironments.getStorm(config);
        String spoutName = "msgConsumer";
        int parallelism = env.getConfig().getInt("envContextConfig.parallelismConfig." + spoutName);
        env.fromSpout(new HiveJobRunningSourcedStormSpoutProvider().getSpout(env.getConfig(), parallelism))
                .withOutputFields(4).nameAs(spoutName).groupBy(Arrays.asList(0))
                .flatMap(new JobConfigurationAdaptorExecutor()).groupBy(Arrays.asList(0))
                .flatMap(new HiveQueryParserExecutor()).groupBy(Arrays.asList(0))
                .flatMap(new HiveResourceSensitivityDataJoinExecutor())
                .alertWithConsumer("hiveAccessLogStream", "hiveAccessAlertByRunningJob");
        env.execute();

    }
}
