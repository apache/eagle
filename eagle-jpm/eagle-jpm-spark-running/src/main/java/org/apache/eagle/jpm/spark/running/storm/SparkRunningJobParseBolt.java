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

package org.apache.eagle.jpm.spark.running.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.eagle.jpm.spark.running.common.SparkRunningConfigManager;
import org.apache.eagle.jpm.spark.running.entities.SparkAppEntity;
import org.apache.eagle.jpm.spark.running.parser.SparkApplicationParser;
import org.apache.eagle.jpm.util.resourceFetch.model.AppInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class SparkRunningJobParseBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SparkRunningJobParseBolt.class);

    private SparkRunningConfigManager.EagleServiceConfig eagleServiceConfig;
    private SparkRunningConfigManager.EndpointConfig endpointConfig;
    private SparkRunningConfigManager.JobExtractorConfig jobExtractorConfig;
    private ExecutorService executorService;
    private Map<String, SparkApplicationParser> runningSparkParsers = new HashMap<>();

    public SparkRunningJobParseBolt(SparkRunningConfigManager.EagleServiceConfig eagleServiceConfig,
                                    SparkRunningConfigManager.EndpointConfig endpointConfig,
                                    SparkRunningConfigManager.JobExtractorConfig jobExtractorConfig) {
        this.eagleServiceConfig = eagleServiceConfig;
        this.endpointConfig = endpointConfig;
        this.jobExtractorConfig = jobExtractorConfig;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.executorService = Executors.newFixedThreadPool(jobExtractorConfig.parseThreadPoolSize);
    }

    @Override
    public void execute(Tuple tuple) {
        AppInfo appInfo = (AppInfo)tuple.getValue(0);
        Map<String, SparkAppEntity> sparkApp = (Map<String, SparkAppEntity>)tuple.getValue(1);

        LOG.info("get spark yarn application " + appInfo.getId());
        //read detailed SparkAppEntity if needed when recover
        //TODO

        SparkApplicationParser applicationParser;
        if (!runningSparkParsers.containsKey(appInfo.getId())) {
            applicationParser = new SparkApplicationParser(eagleServiceConfig, endpointConfig, jobExtractorConfig, appInfo, sparkApp);
            runningSparkParsers.put(appInfo.getId(), applicationParser);
        } else {
            applicationParser = runningSparkParsers.get(appInfo.getId());
        }

        executorService.execute(applicationParser);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }
}
