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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.eagle.jpm.spark.history.config.SparkHistoryCrawlConfig;
import org.apache.eagle.jpm.spark.crawl.JHFInputStreamReader;
import org.apache.eagle.jpm.spark.crawl.SparkApplicationInfo;
import org.apache.eagle.jpm.spark.crawl.SparkFilesystemInputStreamReaderImpl;
import org.apache.eagle.jpm.spark.history.status.JobHistoryZKStateManager;
import org.apache.eagle.jpm.spark.history.status.ZKStateConstant;
import org.apache.eagle.jpm.util.HDFSUtil;
import org.apache.eagle.jpm.util.resourceFetch.ResourceFetcher;
import org.apache.eagle.jpm.util.resourceFetch.SparkHistoryServerResourceFetcher;
import org.apache.eagle.jpm.util.resourceFetch.model.SparkApplication;
import org.apache.eagle.jpm.util.resourceFetch.model.SparkApplicationAttempt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparkJobParseBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(SparkJobParseBolt.class);

    private OutputCollector collector;
    private ResourceFetcher historyServerFetcher;
    private SparkHistoryCrawlConfig config;
    private JobHistoryZKStateManager zkState;
    private Configuration hdfsConf;

    public SparkJobParseBolt(SparkHistoryCrawlConfig config) {
        this.config = config;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.hdfsConf  = new Configuration();
        this.hdfsConf.set("fs.defaultFS", config.hdfsConfig.endpoint);
        this.hdfsConf.setBoolean("fs.hdfs.impl.disable.cache", true);
        this.hdfsConf.set("hdfs.kerberos.principal", config.hdfsConfig.principal);
        this.hdfsConf.set("hdfs.keytab.file", config.hdfsConfig.keytab);
        this.historyServerFetcher = new SparkHistoryServerResourceFetcher(config.jobHistoryConfig.historyServerUrl,
                config.jobHistoryConfig.historyServerUserName, config.jobHistoryConfig.historyServerUserPwd);
        this.zkState = new JobHistoryZKStateManager(config);
    }

    @Override
    public void execute(Tuple tuple) {
        String appId = tuple.getStringByField("appId");
        FileSystem hdfs = null;
        try {
            if (!zkState.hasApplication(appId)) {
                //may already be processed due to some reason
                collector.ack(tuple);
                return;
            }

            SparkApplicationInfo info = zkState.getApplicationInfo(appId);
            //first try to get attempts under the application
            List<SparkApplicationAttempt> attempts = this.getAttemptList(appId);

            if (attempts.isEmpty()) {
                LOG.info("Application:{}( Name:{}, user: {}, queue: {}) not found on history server.", appId, info.getName(), info.getUser(), info.getQueue());
            } else {
                hdfs = HDFSUtil.getFileSystem(this.hdfsConf);
                for (SparkApplicationAttempt attempt : attempts) {
                    String appAttemptLogName = this.getAppAttemptLogName(appId, attempt.getAttemptId());
                    Path attemptFile = getFilePath(appAttemptLogName);
                    JHFInputStreamReader reader = new SparkFilesystemInputStreamReaderImpl(config.info.site, info);
                    reader.read(hdfs.open(attemptFile));
                }
            }

            zkState.updateApplicationStatus(appId, ZKStateConstant.AppStatus.FINISHED);
            LOG.info("Successfully parse application {}", appId);
            collector.ack(tuple);
        } catch (Exception e) {
            LOG.error("Fail to process application {}", appId, e);
            zkState.updateApplicationStatus(appId, ZKStateConstant.AppStatus.FAILED);
            collector.fail(tuple);
        } finally {
            if (null != hdfs) {
                try {
                    hdfs.close();
                } catch (Exception e) {
                    LOG.error("Fail to close hdfs");
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private String getAppAttemptLogName(String appId, String attemptId) {
        if (attemptId.equals("0")) {
            return appId;
        }
        return appId + "_" + attemptId;
    }

    private Path getFilePath(String appAttemptLogName) {
        String attemptLogDir = this.config.hdfsConfig.baseDir + "/" + appAttemptLogName;
        return new Path(attemptLogDir);
    }

    private List<SparkApplicationAttempt> getAttemptList(String appId) throws IOException {
        FileSystem hdfs = null;
        List<SparkApplicationAttempt> attempts = new ArrayList<>();
        try {

            SparkApplication app = null;
            /*try {
                List apps = this.historyServerFetcher.getResource(Constants.ResourceType.SPARK_JOB_DETAIL, appId);
                if (apps != null) {
                    app = (SparkApplication) apps.get(0);
                    attempts = app.getAttempts();
                }
            } catch (Exception e) {
                LOG.warn("Fail to get application detail from history server for appId " + appId, e);
            }*/


            if (null == app) {
                // history server may not have the info, just double check.
                // TODO: if attemptId is not "1, 2, 3,...", we should change the logic.
                // attemptId might be: "appId_000001"
                hdfs = HDFSUtil.getFileSystem(this.hdfsConf);
                int attemptId = 0;

                boolean exists = true;
                while (exists) {
                    String attemptIdString = Integer.toString(attemptId);
                    String appAttemptLogName = this.getAppAttemptLogName(appId, attemptIdString);
                    LOG.info("Attempt ID: {}, App Attempt Log: {}", attemptIdString, appAttemptLogName);
                    Path attemptFile = getFilePath(appAttemptLogName);
                    if (hdfs.exists(attemptFile)) {
                        SparkApplicationAttempt attempt = new SparkApplicationAttempt();
                        attempt.setAttemptId(attemptIdString);
                        attempts.add(attempt);
                    } else if (attemptId > 0) {
                        exists = false;
                    }
                    attemptId++;
                }
            }
            return attempts;
        } finally {
            if (null != hdfs) {
                hdfs.close();
            }
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        zkState.close();
    }
}
