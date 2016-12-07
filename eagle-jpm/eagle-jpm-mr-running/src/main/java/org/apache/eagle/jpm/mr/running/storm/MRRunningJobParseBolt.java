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

import com.typesafe.config.Config;
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
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MRRunningJobParseBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MRRunningJobParseBolt.class);

    private MRRunningJobConfig.EndpointConfig endpointConfig;
    private MRRunningJobConfig.ZKStateConfig zkStateConfig;
    private ExecutorService executorService;
    private Map<String, MRJobParser> runningMRParsers;
    private transient MRRunningJobManager runningJobManager;
    private MRRunningJobConfig.EagleServiceConfig eagleServiceConfig;
    private ResourceFetcher resourceFetcher;
    private List<String> configKeys;
    private Config config;

    public MRRunningJobParseBolt(MRRunningJobConfig.EagleServiceConfig eagleServiceConfig,
                                 MRRunningJobConfig.EndpointConfig endpointConfig,
                                 MRRunningJobConfig.ZKStateConfig zkStateConfig,
                                 List<String> configKeys,
                                 Config config) {
        this.eagleServiceConfig = eagleServiceConfig;
        this.endpointConfig = endpointConfig;
        this.runningMRParsers = new HashMap<>();
        this.zkStateConfig = zkStateConfig;
        this.configKeys = configKeys;
        this.config = config;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.executorService = Executors.newFixedThreadPool(endpointConfig.parseJobThreadPoolSize);

        this.runningJobManager = new MRRunningJobManager(zkStateConfig);
        this.resourceFetcher = new RMResourceFetcher(endpointConfig.rmUrls);
    }

    @Override
    public void execute(Tuple tuple) {
        AppInfo appInfo = (AppInfo) tuple.getValue(1);
        Map<String, JobExecutionAPIEntity> mrJobs = (Map<String, JobExecutionAPIEntity>) tuple.getValue(2);

        LOG.info("get mr yarn application " + appInfo.getId());

        MRJobParser applicationParser;
        if (!runningMRParsers.containsKey(appInfo.getId())) {
            applicationParser = new MRJobParser(endpointConfig, eagleServiceConfig,
                    appInfo, mrJobs, runningJobManager, this.resourceFetcher, configKeys, this.config);
            runningMRParsers.put(appInfo.getId(), applicationParser);
            LOG.info("create application parser for {}", appInfo.getId());
        } else {
            applicationParser = runningMRParsers.get(appInfo.getId());
            applicationParser.setAppInfo(appInfo);
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
        runningJobManager.close();
    }
}
