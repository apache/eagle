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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.collections.map.HashedMap;
import org.apache.eagle.jpm.mr.running.config.MRRunningConfigManager;
import org.apache.eagle.jpm.mr.running.entities.JobExecutionAPIEntity;
import org.apache.eagle.jpm.mr.running.parser.MRJobEntityCreationHandler;
import org.apache.eagle.jpm.mr.running.parser.MRJobParser;
import org.apache.eagle.jpm.mr.running.recover.RunningJobManager;
import org.apache.eagle.jpm.util.resourceFetch.model.AppInfo;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MRRunningJobParseBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MRRunningJobParseBolt.class);

    private MRRunningConfigManager.EndpointConfig endpointConfig;
    private MRRunningConfigManager.JobExtractorConfig jobExtractorConfig;
    private MRRunningConfigManager.ZKStateConfig zkStateConfig;
    private ExecutorService executorService;
    private Map<String, MRJobParser> runningMRParsers;
    private transient RunningJobManager runningJobManager;
    private MRJobEntityCreationHandler mrJobEntityCreationHandler;
    private MRRunningConfigManager.EagleServiceConfig eagleServiceConfig;
    public MRRunningJobParseBolt(MRRunningConfigManager.EagleServiceConfig eagleServiceConfig,
                                 MRRunningConfigManager.EndpointConfig endpointConfig,
                                 MRRunningConfigManager.JobExtractorConfig jobExtractorConfig,
                                 MRRunningConfigManager.ZKStateConfig zkStateConfig) {
        this.eagleServiceConfig = eagleServiceConfig;
        this.endpointConfig = endpointConfig;
        this.jobExtractorConfig = jobExtractorConfig;
        this.runningMRParsers = new HashMap<>();
        this.zkStateConfig = zkStateConfig;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.executorService = Executors.newFixedThreadPool(jobExtractorConfig.parseJobThreadPoolSize);
        this.runningJobManager = new RunningJobManager(zkStateConfig);
        this.mrJobEntityCreationHandler = new MRJobEntityCreationHandler(eagleServiceConfig);
    }

    @Override
    public void execute(Tuple tuple) {
        AppInfo appInfo = (AppInfo)tuple.getValue(1);
        Map<String, JobExecutionAPIEntity> mrJobs = (Map<String, JobExecutionAPIEntity>)tuple.getValue(2);

        LOG.info("get mr yarn application " + appInfo.getId());

        Set<String> runningParserIds = new HashSet<>(runningMRParsers.keySet());
        runningParserIds.stream()
                .filter(appId -> runningMRParsers.get(appId).status() == MRJobParser.ParserStatus.FINISHED)
                .forEach(appId -> runningMRParsers.remove(appId));

        MRJobParser applicationParser;
        if (!runningMRParsers.containsKey(appInfo.getId())) {
            applicationParser = new MRJobParser(endpointConfig, jobExtractorConfig, mrJobEntityCreationHandler, appInfo, mrJobs, runningJobManager);
            runningMRParsers.put(appInfo.getId(), applicationParser);
            LOG.info("create application parser for {}", appInfo.getId());
        } else {
            applicationParser = runningMRParsers.get(appInfo.getId());
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
