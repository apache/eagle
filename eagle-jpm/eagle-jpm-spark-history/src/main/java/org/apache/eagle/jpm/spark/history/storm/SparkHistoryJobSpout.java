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

package org.apache.eagle.jpm.spark.history.storm;

import org.apache.eagle.jpm.spark.history.SparkHistoryJobAppConfig;
import org.apache.eagle.jpm.spark.history.status.JobHistoryZKStateManager;
import org.apache.eagle.jpm.spark.history.status.ZKStateConstant;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourcefetch.RMResourceFetcher;
import org.apache.eagle.jpm.util.resourcefetch.ResourceFetcher;
import org.apache.eagle.jpm.util.resourcefetch.model.AppInfo;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class SparkHistoryJobSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(SparkHistoryJobSpout.class);
    private SpoutOutputCollector collector;
    private JobHistoryZKStateManager zkState;
    private SparkHistoryJobAppConfig config;
    private ResourceFetcher rmFetch;
    private long lastFinishAppTime = 0;

    public SparkHistoryJobSpout(SparkHistoryJobAppConfig config) {
        this.config = config;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        rmFetch = new RMResourceFetcher(config.jobHistoryConfig.rms);
        this.collector = spoutOutputCollector;
        this.zkState = new JobHistoryZKStateManager(config);
        this.lastFinishAppTime = zkState.readLastFinishedTimestamp();
        zkState.resetApplications();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void nextTuple() {
        //LOG.info("Start to run tuple");
        try {
            Calendar calendar = Calendar.getInstance();
            long fetchTime = calendar.getTimeInMillis();
            calendar.setTimeInMillis(this.lastFinishAppTime);
            if (fetchTime - this.lastFinishAppTime > this.config.stormConfig.spoutCrawlInterval) {
                LOG.info("Last finished time = {}", calendar.getTime());
                List<AppInfo> appInfos = rmFetch.getResource(Constants.ResourceType.COMPLETE_SPARK_JOB, Long.toString(lastFinishAppTime));
                if (appInfos != null) {
                    LOG.info("Get " + appInfos.size() + " from yarn resource manager.");
                    for (AppInfo app : appInfos) {
                        String appId = app.getId();
                        if (!zkState.hasApplication(appId)) {
                            zkState.addFinishedApplication(appId, app.getQueue(), app.getState(), app.getFinalStatus(), app.getUser(), app.getName());
                        }
                    }
                }
                this.lastFinishAppTime = fetchTime;
                zkState.updateLastUpdateTime(fetchTime);
            }

            List<String> appIds = zkState.loadApplications(10);
            for (String appId: appIds) {
                collector.emit(new Values(appId), appId);
                LOG.info("emit " + appId);
                zkState.updateApplicationStatus(appId, ZKStateConstant.AppStatus.SENT_FOR_PARSE);
            }

            if (appIds.isEmpty()) {
                this.takeRest(5);
            } else {
                LOG.info("{} apps sent.", appIds.size());
            }
        } catch (Exception e) {
            LOG.error("Fail to run next tuple", e);
        }
    }

    private void takeRest(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            LOG.warn("exception found", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("appId"));
    }

    @Override
    public void fail(Object msgId) {
        // Sleep 3 seconds and retry.
        // Utils.sleep(3000);
        collector.emit(new Values(msgId), msgId);
        zkState.updateApplicationStatus((String)msgId, ZKStateConstant.AppStatus.FAILED);
        LOG.warn("fail {}", msgId.toString());
    }

    @Override
    public void ack(Object msgId) {
        zkState.updateApplicationStatus((String)msgId, ZKStateConstant.AppStatus.FINISHED);
        LOG.info("ack {}", msgId.toString());
    }

    @Override
    public void close() {
        super.close();
        zkState.close();
    }
}
