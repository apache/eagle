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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.eagle.jpm.spark.running.common.SparkRunningConfigManager;
import org.apache.eagle.jpm.spark.running.entities.SparkAppEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourceFetch.RMResourceFetcher;
import org.apache.eagle.jpm.util.resourceFetch.ResourceFetcher;
import org.apache.eagle.jpm.util.resourceFetch.model.AppInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkRunningJobFetchSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(SparkRunningJobFetchSpout.class);

    private SparkRunningConfigManager.JobExtractorConfig jobExtractorConfig;
    private SparkRunningConfigManager.EndpointConfig endpointConfig;
    private ResourceFetcher resourceFetcher;
    private SpoutOutputCollector collector;
    private boolean inited;

    public SparkRunningJobFetchSpout(SparkRunningConfigManager.JobExtractorConfig jobExtractorConfig,
                                     SparkRunningConfigManager.EndpointConfig endpointConfig) {
        this.jobExtractorConfig = jobExtractorConfig;
        this.endpointConfig = endpointConfig;
        this.inited = false;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        resourceFetcher = new RMResourceFetcher(endpointConfig.rmUrls);
        collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        LOG.info("Start to fetch spark running jobs");
        try {
            List<AppInfo> apps;
            Map<String, Map<String, SparkAppEntity>> sparkApps = null;
            if (!this.inited) {
                sparkApps = recoverRunningApps();
                LOG.info("recover {} spark yarn apps from zookeeper", sparkApps.size());
                apps = new ArrayList<>();
                for (String appId : sparkApps.keySet()) {
                    apps.add(sparkApps.get(appId).get(0).getAppInfo());
                }
                this.inited = true;
            } else {
                apps = resourceFetcher.getResource(Constants.ResourceType.RUNNING_SPARK_JOB);
                LOG.info("get {} apps from resource manager", apps.size());
            }

            for (int i = 0; i < apps.size(); i++) {
                LOG.info("emit spark yarn application " + apps.get(i).getId());
                if (sparkApps != null) {
                    //emit (AppInfo, Map<String, SparkAppEntity>)
                    collector.emit(new Values(apps.get(i).getId(), apps.get(i), sparkApps.get(apps.get(i).getId())));
                } else {
                    collector.emit(new Values(apps.get(i).getId(), apps.get(i), null));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                Thread.sleep(jobExtractorConfig.fetchRunningJobInterval * 1000);
            } catch (Exception e) {
            }
        }
    }

    private Map<String, Map<String, SparkAppEntity>> recoverRunningApps() {
        //we need read from zookeeper, path looks like /apps/spark/running/yarnAppId/appId/
        //content of path /apps/spark/running/yarnAppId/appId is SparkAppEntity(current attempt)
        //as we know, a yarn application may contains many spark applications
        //so, the returned results is a Map, key is yarn appId
        Map<String, Map<String, SparkAppEntity>> result = new HashMap<>();
        //TODO
        return result;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("appId", "appInfo", "sparkAppEntity"));
    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void close() {

    }
}
