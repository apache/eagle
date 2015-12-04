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
package org.apache.eagle.security.hive.jobrunning;

import org.apache.eagle.datastream.ExecutionEnvironments;
import org.apache.eagle.datastream.StormExecutionEnvironment;
import org.apache.eagle.security.hive.sensitivity.HiveResourceSensitivityDataJoinExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;

public class HiveJobRunningMonitoringMain {
	private static final Logger LOG = LoggerFactory.getLogger(HiveJobRunningMonitoringMain.class);

	public static void main(String[] args) throws Exception{
        StormExecutionEnvironment env = ExecutionEnvironments.getStorm(args);
        String spoutName = "msgConsumer";
        int parallelism = env.getConfig().getInt("envContextConfig.parallelismConfig." + spoutName);
        env.from(new HiveJobRunningSourcedStormSpoutProvider().getSpout(env.getConfig(), parallelism))
                .renameOutputFields(4).name(spoutName).groupBy(Arrays.asList(0))
                .flatMap(new JobConfigurationAdaptorExecutor()).groupBy(Arrays.asList(0))
                .flatMap(new HiveQueryParserExecutor()).groupBy(Arrays.asList(0))
                .flatMap(new HiveResourceSensitivityDataJoinExecutor())
                .alertWithConsumer("hiveAccessLogStream", "hiveAccessAlertByRunningJob");
        env.execute();
	}
}
