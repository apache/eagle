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

import org.apache.eagle.jpm.spark.running.SparkRunningJobAppConfig;
import org.apache.eagle.jpm.spark.running.entities.SparkAppEntity;
import org.apache.eagle.jpm.spark.running.parser.SparkApplicationParser;
import org.apache.eagle.jpm.spark.running.recover.SparkRunningJobManager;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourcefetch.RMResourceFetcher;
import org.apache.eagle.jpm.util.resourcefetch.ResourceFetcher;
import org.apache.eagle.jpm.util.resourcefetch.model.AppInfo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SparkRunningJobParseBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SparkRunningJobParseBolt.class);

    private SparkRunningJobAppConfig.ZKStateConfig zkStateConfig;
    private SparkRunningJobAppConfig.EagleServiceConfig eagleServiceConfig;
    private SparkRunningJobAppConfig.EndpointConfig endpointConfig;
    private SparkRunningJobAppConfig.JobExtractorConfig jobExtractorConfig;
    private ExecutorService executorService;
    private Map<String, SparkApplicationParser> runningSparkParsers;
    private ResourceFetcher resourceFetcher;
    public SparkRunningJobParseBolt(SparkRunningJobAppConfig.ZKStateConfig zkStateConfig,
                                    SparkRunningJobAppConfig.EagleServiceConfig eagleServiceConfig,
                                    SparkRunningJobAppConfig.EndpointConfig endpointConfig,
                                    SparkRunningJobAppConfig.JobExtractorConfig jobExtractorConfig) {
        this.zkStateConfig = zkStateConfig;
        this.eagleServiceConfig = eagleServiceConfig;
        this.endpointConfig = endpointConfig;
        this.jobExtractorConfig = jobExtractorConfig;
        this.runningSparkParsers = new HashMap<>();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.executorService = Executors.newFixedThreadPool(jobExtractorConfig.parseThreadPoolSize);
        this.resourceFetcher = new RMResourceFetcher(endpointConfig.rmUrls);
    }

    @Override
    public void execute(Tuple tuple) {
        AppInfo appInfo = (AppInfo)tuple.getValue(1);
        Map<String, SparkAppEntity> sparkApp = (Map<String, SparkAppEntity>)tuple.getValue(2);

        LOG.info("get spark yarn application " + appInfo.getId());

        SparkApplicationParser applicationParser;
        if (!runningSparkParsers.containsKey(appInfo.getId())) {
            applicationParser = new SparkApplicationParser(eagleServiceConfig, endpointConfig, jobExtractorConfig, appInfo, sparkApp, new SparkRunningJobManager(zkStateConfig), resourceFetcher);
            runningSparkParsers.put(appInfo.getId(), applicationParser);
            LOG.info("create application parser for {}", appInfo.getId());
        } else {
            applicationParser = runningSparkParsers.get(appInfo.getId());
        }

        Set<String> runningParserIds = new HashSet<>(runningSparkParsers.keySet());
        runningParserIds.stream()
                .filter(appId -> runningSparkParsers.get(appId).status() == SparkApplicationParser.ParserStatus.APP_FINISHED)
                .forEach(appId -> {
                    runningSparkParsers.remove(appId);
                    LOG.info("remove parser {}", appId);
                });

        if (appInfo.getState().equals(Constants.AppState.FINISHED.toString()) ||
                applicationParser.status() == SparkApplicationParser.ParserStatus.FINISHED) {
            applicationParser.setStatus(SparkApplicationParser.ParserStatus.RUNNING);
            executorService.execute(applicationParser);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }
}
