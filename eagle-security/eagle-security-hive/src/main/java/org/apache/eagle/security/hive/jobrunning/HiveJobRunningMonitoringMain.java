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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.apache.eagle.dataproc.util.ConfigOptionParser;
import org.apache.eagle.datastream.ExecutionEnvironmentFactory;
import org.apache.eagle.datastream.StormExecutionEnvironment;
import org.apache.eagle.security.hive.sensitivity.HiveResourceSensitivityDataJoinExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;

public class HiveJobRunningMonitoringMain {
	private static final Logger LOG = LoggerFactory.getLogger(HiveJobRunningMonitoringMain.class);

	public static void main(String[] args) throws Exception{
        Config config = new ConfigOptionParser().load(args);

        LOG.info("Config class: " + config.getClass().getCanonicalName());

        if(LOG.isDebugEnabled()) LOG.debug("Config content:"+config.root().render(ConfigRenderOptions.concise()));

        String spoutName = "msgConsumer";
        int parallelism = config.getInt("envContextConfig.parallelismConfig." + spoutName);
        StormExecutionEnvironment env = ExecutionEnvironmentFactory.getStorm(config);
        env.newSource(new HiveJobRunningSourcedStormSpoutProvider().getSpout(config, parallelism)).renameOutputFields(4).withName(spoutName).groupBy(Arrays.asList(0))
                .flatMap(new JobConfigurationAdaptorExecutor()).groupBy(Arrays.asList(0))
                .flatMap(new HiveQueryParserExecutor()).groupBy(Arrays.asList(0))
                .flatMap(new HiveResourceSensitivityDataJoinExecutor())
                .alertWithConsumer("hiveAccessLogStream", "hiveAccessAlertByRunningJob");
        env.execute();
	}
};