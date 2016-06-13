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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.eagle.jpm.spark.history.config.SparkHistoryCrawlConfig;
import org.apache.eagle.jpm.spark.history.status.JobHistoryZKStateManager;
import org.apache.eagle.jpm.spark.history.status.ZKStateConstant;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourceFetch.RMResourceFetcher;
import org.apache.eagle.jpm.util.resourceFetch.ResourceFetcher;
import org.apache.eagle.jpm.util.resourceFetch.model.AppInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class FinishedSparkJobSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(FinishedSparkJobSpout.class);
    private SpoutOutputCollector collector;
    private JobHistoryZKStateManager zkState;
    private SparkHistoryCrawlConfig config;
    private ResourceFetcher rmFetch;
    private long lastFinishAppTime = 0;
    private Map<String, Integer> failTimes;

    private static final int FAIL_MAX_TIMES = 5;

    public FinishedSparkJobSpout(SparkHistoryCrawlConfig config){
        this.config = config;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        rmFetch = new RMResourceFetcher(config.jobHistoryConfig.rms);
        this.failTimes = new HashMap<>();
        this.collector = spoutOutputCollector;
        this.zkState = new JobHistoryZKStateManager(config);
        this.lastFinishAppTime = zkState.readLastFinishedTimestamp();
        zkState.resetApplications();
    }


    @Override
    public void nextTuple() {
        LOG.info("Start to run tuple");
        try {
            long fetchTime = Calendar.getInstance().getTimeInMillis();
            if (fetchTime - this.lastFinishAppTime > 5 * 60 * 1000) {
                List apps = rmFetch.getResource(Constants.ResourceType.COMPLETE_SPARK_JOB, new Long(lastFinishAppTime).toString());
                List<AppInfo> appInfos = (null != apps ? (List<AppInfo>)apps.get(0):new ArrayList<AppInfo>());
                LOG.info("Get " + appInfos.size() + " from yarn resource manager.");
                for (AppInfo app: appInfos) {
                    String appId = app.getId();
                    if (!zkState.hasApplication(appId)) {
                        zkState.addFinishedApplication(appId, app.getQueue(), app.getState(), app.getFinalStatus(), app.getUser(), app.getName());
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
            LOG.info("{} apps sent.", appIds.size());

            if (appIds.isEmpty()) {
                this.takeRest(60);
            }
        } catch (Exception e) {
            LOG.error("Fail to run next tuple", e);
           // this.takeRest(10);
        }

    }

    private void takeRest(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch(InterruptedException e) {
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("appId"));
    }

    @Override
    public void fail(Object msgId) {
        String appId = (String) msgId;
        int failTimes = 0;
        if (this.failTimes.containsKey(appId)) {
            failTimes = this.failTimes.get(appId);
        }
        failTimes ++;
        if (failTimes >= FAIL_MAX_TIMES) {
            this.failTimes.remove(appId);
            zkState.updateApplicationStatus(appId, ZKStateConstant.AppStatus.FINISHED);
            LOG.error(String.format("Application %s has failed for over %s times, drop it.", appId, FAIL_MAX_TIMES));
        } else {
            this.failTimes.put(appId, failTimes);
            collector.emit(new Values(appId), appId);
            zkState.updateApplicationStatus(appId, ZKStateConstant.AppStatus.SENT_FOR_PARSE);
        }
    }

    @Override
    public void ack(Object msgId) {
        String appId = (String) msgId;
        if (this.failTimes.containsKey(appId)) {
            this.failTimes.remove(appId);
        }

    }

    @Override
    public void close() {
        super.close();
        zkState.close();
    }
}
