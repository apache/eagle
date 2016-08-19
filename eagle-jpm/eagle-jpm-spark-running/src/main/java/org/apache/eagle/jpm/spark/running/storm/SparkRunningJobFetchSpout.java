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
import org.apache.eagle.jpm.spark.running.SparkRunningJobAppConfig;
import org.apache.eagle.jpm.spark.running.entities.SparkAppEntity;
import org.apache.eagle.jpm.spark.running.recover.SparkRunningJobManager;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourceFetch.RMResourceFetcher;
import org.apache.eagle.jpm.util.resourceFetch.ResourceFetcher;
import org.apache.eagle.jpm.util.resourceFetch.model.AppInfo;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SparkRunningJobFetchSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(SparkRunningJobFetchSpout.class);

    private SparkRunningJobAppConfig.ZKStateConfig zkStateConfig;
    private SparkRunningJobAppConfig.JobExtractorConfig jobExtractorConfig;
    private SparkRunningJobAppConfig.EndpointConfig endpointConfig;
    private ResourceFetcher resourceFetcher;
    private SpoutOutputCollector collector;
    private boolean init;
    private transient SparkRunningJobManager sparkRunningJobManager;
    private Set<String> runningYarnApps;

    public SparkRunningJobFetchSpout(SparkRunningJobAppConfig.JobExtractorConfig jobExtractorConfig,
                                     SparkRunningJobAppConfig.EndpointConfig endpointConfig,
                                     SparkRunningJobAppConfig.ZKStateConfig zkStateConfig) {
        this.jobExtractorConfig = jobExtractorConfig;
        this.endpointConfig = endpointConfig;
        this.zkStateConfig = zkStateConfig;
        this.init = !(zkStateConfig.recoverEnabled);
        this.runningYarnApps = new HashSet<>();
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        resourceFetcher = new RMResourceFetcher(endpointConfig.rmUrls);
        collector = spoutOutputCollector;
        this.sparkRunningJobManager = new SparkRunningJobManager(zkStateConfig);
    }

    @Override
    public void nextTuple() {
        LOG.info("Start to fetch spark running jobs");
        try {
            Map<String, Map<String, SparkAppEntity>> sparkApps = null;
            List<AppInfo> apps;
            if (!this.init) {
                sparkApps = recoverRunningApps();

                apps = new ArrayList<>();
                for (String appId : sparkApps.keySet()) {
                    Map<String, SparkAppEntity> jobs = sparkApps.get(appId);
                    if (jobs.size() > 0) {
                        Set<String> jobIds = jobs.keySet();
                        apps.add(jobs.get(jobIds.iterator().next()).getAppInfo());
                        this.runningYarnApps.add(appId);
                    }
                }
                LOG.info("recover {} spark yarn apps from zookeeper", apps.size());
                this.init = true;
            } else {
                apps = resourceFetcher.getResource(Constants.ResourceType.RUNNING_SPARK_JOB);
                LOG.info("get {} apps from resource manager", apps == null ? 0 : apps.size());
                Set<String> running = new HashSet<>();
                Iterator<String> appIdIterator = this.runningYarnApps.iterator();
                while (appIdIterator.hasNext()) {
                    String appId = appIdIterator.next();
                    boolean hasFinished = true;
                    if (apps != null) {
                        for (AppInfo appInfo : apps) {
                            if (appId.equals(appInfo.getId())) {
                                hasFinished = false;
                            }
                            running.add(appInfo.getId());
                        }

                        if (hasFinished) {
                            try {
                                Map<String, SparkAppEntity> result = this.sparkRunningJobManager.recoverYarnApp(appId);
                                if (result.size() > 0) {
                                    if (sparkApps == null) {
                                        sparkApps = new HashMap<>();
                                    }
                                    sparkApps.put(appId, result);
                                    AppInfo appInfo = result.get(result.keySet().iterator().next()).getAppInfo();
                                    appInfo.setState(Constants.AppState.FINISHED.toString());
                                    apps.add(appInfo);
                                }
                            } catch (KeeperException.NoNodeException e) {
                                LOG.warn("{}", e);
                                LOG.warn("yarn app {} has finished", appId);
                            }
                        }
                    }
                }

                this.runningYarnApps = running;
                LOG.info("get {} total apps(contains finished)", apps == null ? 0 : apps.size());
            }

            if (apps != null) {
                for (AppInfo app : apps) {
                    LOG.info("emit spark yarn application " + app.getId());
                    if (sparkApps != null) {
                        //emit (AppInfo, Map<String, SparkAppEntity>)
                        collector.emit(new Values(app.getId(), app, sparkApps.get(app.getId())));
                    } else {
                        collector.emit(new Values(app.getId(), app, null));
                    }
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
        Map<String, Map<String, SparkAppEntity>> result = this.sparkRunningJobManager.recover();
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
