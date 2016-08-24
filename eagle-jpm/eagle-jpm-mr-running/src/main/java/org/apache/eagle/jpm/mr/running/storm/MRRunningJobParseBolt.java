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

package org.apache.eagle.jpm.mr.running.storm;

import org.apache.eagle.jpm.mr.running.MRRunningJobConfig;
import org.apache.eagle.jpm.mr.running.parser.MRJobParser;
import org.apache.eagle.jpm.mr.running.recover.MRRunningJobManager;
import org.apache.eagle.jpm.mr.runningentity.JobExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourcefetch.RMResourceFetcher;
import org.apache.eagle.jpm.util.resourcefetch.ResourceFetcher;
import org.apache.eagle.jpm.util.resourcefetch.model.AppInfo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.topology.base.BaseRichBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MRRunningJobParseBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MRRunningJobParseBolt.class);

    private MRRunningJobConfig.EndpointConfig endpointConfig;
    private MRRunningJobConfig.JobExtractorConfig jobExtractorConfig;
    private MRRunningJobConfig.ZKStateConfig zkStateConfig;
    private ExecutorService executorService;
    private Map<String, MRJobParser> runningMRParsers;
    private transient MRRunningJobManager runningJobManager;
    private MRRunningJobConfig.EagleServiceConfig eagleServiceConfig;
    private ResourceFetcher resourceFetcher;
    private List<String> configKeys;

    public MRRunningJobParseBolt(MRRunningJobConfig.EagleServiceConfig eagleServiceConfig,
                                 MRRunningJobConfig.EndpointConfig endpointConfig,
                                 MRRunningJobConfig.JobExtractorConfig jobExtractorConfig,
                                 MRRunningJobConfig.ZKStateConfig zkStateConfig,
                                 List<String> configKeys) {
        this.eagleServiceConfig = eagleServiceConfig;
        this.endpointConfig = endpointConfig;
        this.jobExtractorConfig = jobExtractorConfig;
        this.runningMRParsers = new HashMap<>();
        this.zkStateConfig = zkStateConfig;
        this.configKeys = configKeys;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.executorService = Executors.newFixedThreadPool(jobExtractorConfig.parseJobThreadPoolSize);

        this.runningJobManager = new MRRunningJobManager(zkStateConfig);
        this.resourceFetcher = new RMResourceFetcher(endpointConfig.rmUrls);
    }

    @Override
    public void execute(Tuple tuple) {
        AppInfo appInfo = (AppInfo)tuple.getValue(1);
        Map<String, JobExecutionAPIEntity> mrJobs = (Map<String, JobExecutionAPIEntity>)tuple.getValue(2);

        LOG.info("get mr yarn application " + appInfo.getId());

        MRJobParser applicationParser;
        if (!runningMRParsers.containsKey(appInfo.getId())) {
            applicationParser = new MRJobParser(jobExtractorConfig, eagleServiceConfig,
                    appInfo, mrJobs, runningJobManager, this.resourceFetcher, configKeys);
            runningMRParsers.put(appInfo.getId(), applicationParser);
            LOG.info("create application parser for {}", appInfo.getId());
        } else {
            applicationParser = runningMRParsers.get(appInfo.getId());
        }

        Set<String> runningParserIds = new HashSet<>(runningMRParsers.keySet());
        runningParserIds.stream()
                .filter(appId -> runningMRParsers.get(appId).status() == MRJobParser.ParserStatus.APP_FINISHED)
                .forEach(appId -> {
                    runningMRParsers.remove(appId);
                    LOG.info("remove parser {}", appId);
                });

        if (appInfo.getState().equals(Constants.AppState.FINISHED.toString())
            || applicationParser.status() == MRJobParser.ParserStatus.FINISHED) {
            applicationParser.setStatus(MRJobParser.ParserStatus.RUNNING);
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
