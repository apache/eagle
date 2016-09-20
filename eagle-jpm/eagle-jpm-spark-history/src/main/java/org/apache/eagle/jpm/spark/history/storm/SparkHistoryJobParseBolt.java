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
import org.apache.eagle.jpm.spark.history.crawl.JHFInputStreamReader;
import org.apache.eagle.jpm.spark.history.crawl.SparkApplicationInfo;
import org.apache.eagle.jpm.spark.history.crawl.SparkFilesystemInputStreamReaderImpl;
import org.apache.eagle.jpm.spark.history.status.JobHistoryZKStateManager;
import org.apache.eagle.jpm.spark.history.status.ZKStateConstant;
import org.apache.eagle.jpm.util.HDFSUtil;
import org.apache.eagle.jpm.util.resourcefetch.ResourceFetcher;
import org.apache.eagle.jpm.util.resourcefetch.SparkHistoryServerResourceFetcher;
import org.apache.eagle.jpm.util.resourcefetch.model.SparkApplication;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class SparkHistoryJobParseBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(SparkHistoryJobParseBolt.class);

    private OutputCollector collector;
    private ResourceFetcher historyServerFetcher;
    private SparkHistoryJobAppConfig config;
    private JobHistoryZKStateManager zkState;
    private Configuration hdfsConf;

    public SparkHistoryJobParseBolt(SparkHistoryJobAppConfig config) {
        this.config = config;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.hdfsConf = new Configuration();
        this.hdfsConf.setBoolean("fs.hdfs.impl.disable.cache", true);
        for (Map.Entry<String, String> entry : config.jobHistoryConfig.hdfs.entrySet()) {
            this.hdfsConf.set(entry.getKey(), entry.getValue());
            LOG.info("conf key {}, conf value {}", entry.getKey(), entry.getValue());
        }
        this.historyServerFetcher = new SparkHistoryServerResourceFetcher(config.jobHistoryConfig.historyServerUrl,
                config.jobHistoryConfig.historyServerUserName, config.jobHistoryConfig.historyServerUserPwd);
        this.zkState = new JobHistoryZKStateManager(config);
    }

    @Override
    public void execute(Tuple tuple) {
        String appId = tuple.getStringByField("appId");
        if (!zkState.hasApplication(appId)) {
            //may already be processed due to some reason
            collector.ack(tuple);
            return;
        }

        try (FileSystem hdfs = HDFSUtil.getFileSystem(this.hdfsConf)) {
            SparkApplicationInfo info = zkState.getApplicationInfo(appId);
            //first try to get attempts under the application

            Set<String> inprogressSet = new HashSet<String>();
            List<String> attemptLogNames = this.getAttemptLogNameList(appId, hdfs, inprogressSet);

            if (attemptLogNames.isEmpty()) {
                LOG.info("Application:{}( Name:{}, user: {}, queue: {}) not found on history server.",
                    appId, info.getName(), info.getUser(), info.getQueue());
            } else {
                for (String attemptLogName : attemptLogNames) {
                    String extension = "";
                    if (inprogressSet.contains(attemptLogName)) {
                        extension = ".inprogress";
                    }
                    LOG.info("Attempt log name: " + attemptLogName + extension);

                    Path attemptFile = getFilePath(attemptLogName, extension);
                    JHFInputStreamReader reader = new SparkFilesystemInputStreamReaderImpl(config, info);
                    reader.read(hdfs.open(attemptFile));
                }
            }

            zkState.updateApplicationStatus(appId, ZKStateConstant.AppStatus.FINISHED);
            LOG.info("Successfully parse application {}", appId);
            collector.ack(tuple);
        } catch (RuntimeException e) {
            LOG.warn("fail to process application {} due to RuntimeException, ignore it", appId, e);
            zkState.updateApplicationStatus(appId, ZKStateConstant.AppStatus.FINISHED);
            collector.ack(tuple);
        } catch (Exception e) {
            LOG.error("Fail to process application {}, and retry", appId, e);
            collector.fail(tuple);
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

    private Path getFilePath(String appAttemptLogName, String extension) {
        String attemptLogDir = this.config.jobHistoryConfig.baseDir + "/" + appAttemptLogName + extension;
        return new Path(attemptLogDir);
    }

    private List<String> getAttemptLogNameList(String appId, FileSystem hdfs, Set<String> inprogressSet)
            throws IOException {
        List<String> attempts = new ArrayList<String>();
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
            // Use getResourceManagerVersion() to compare YARN/RM versions.
            // attemptId might be: "appId_000001"
            int attemptId = 0;

            boolean exists = true;
            while (exists) {
                // For Yarn version 2.4.x
                // log name: application_1464382345557_269065_1
                String attemptIdString = Integer.toString(attemptId);

                // For Yarn version >= 2.7,
                // log name: "application_1468625664674_0003_appattempt_1468625664674_0003_000001"
                // String attemptIdFormatted = String.format("%06d", attemptId);
                //
                // // remove "application_" to get the number part of appID.
                // String sparkAppIdNum = appId.substring(12);
                // String attemptIdString = "appattempt_" + sparkAppIdNum + "_" + attemptIdFormatted;

                String appAttemptLogName = this.getAppAttemptLogName(appId, attemptIdString);
                LOG.info("Attempt ID: {}, App Attempt Log: {}", attemptIdString, appAttemptLogName);

                String extension = "";
                Path attemptFile = getFilePath(appAttemptLogName, extension);
                extension = ".inprogress";
                Path inprogressFile = getFilePath(appAttemptLogName, extension);
                Path logFile = null;
                // Check if history log exists.
                if (hdfs.exists(attemptFile)) {
                    logFile = attemptFile;
                } else if (hdfs.exists(inprogressFile)) {
                    logFile = inprogressFile;
                    inprogressSet.add(appAttemptLogName);
                } else if (attemptId > 0) {
                    exists = false;
                }

                if (logFile != null) {
                    attempts.add(appAttemptLogName);
                }
                attemptId++;
            }
        }
        return attempts;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        zkState.close();
    }
}