/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.analyzer.publisher;

import com.typesafe.config.Config;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.eagle.app.service.ApplicationEmailService;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.mail.AlertEmailContext;
import org.apache.eagle.jpm.analyzer.AnalyzerEntity;
import org.apache.eagle.jpm.analyzer.publisher.dedup.AlertDeduplicator;
import org.apache.eagle.jpm.analyzer.publisher.dedup.impl.SimpleDeduplicator;
import org.apache.eagle.jpm.analyzer.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EmailPublisher implements Publisher, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(EmailPublisher.class);

    private Config config;
    private AlertDeduplicator alertDeduplicator;

    public EmailPublisher(Config config) {
        this.config = config;
        this.alertDeduplicator = new SimpleDeduplicator();
    }

    @Override
    public void publish(AnalyzerEntity analyzerJobEntity, Result result) {
        if (result.getAlertMessages().size() == 0) {
            return;
        }

        LOG.info("EmailPublisher gets job {}", analyzerJobEntity.getJobDefId());
        if (alertDeduplicator.dedup(analyzerJobEntity, result)) {
            LOG.info("skip job {} alert because it is duplicated", analyzerJobEntity.getJobDefId());
            return;
        }

        Map<String, String> basic = new HashMap<>();
        basic.put("site", analyzerJobEntity.getSiteId());
        basic.put("name", analyzerJobEntity.getJobDefId());
        basic.put("user", analyzerJobEntity.getUserId());
        basic.put("status", analyzerJobEntity.getCurrentState());
        basic.put("duration", analyzerJobEntity.getDurationTime() * 1.0 / 1000 + "s");
        basic.put("start", DateTimeUtil.millisecondsToHumanDateWithSeconds(analyzerJobEntity.getStartTime()));
        basic.put("end", analyzerJobEntity.getEndTime() == 0
                ? "0"
                : DateTimeUtil.millisecondsToHumanDateWithSeconds(analyzerJobEntity.getEndTime()));
        basic.put("progress", analyzerJobEntity.getProgress() + "%");
        basic.put("detail", getJobLink(analyzerJobEntity));


        Map<String, Map<String, String>> extend = new HashMap<>();
        Map<String, List<Pair<Result.ResultLevel, String>>> alertMessages = result.getAlertMessages();
        for (String evaluator : alertMessages.keySet()) {
            List<Pair<Result.ResultLevel, String>> messages = alertMessages.get(evaluator);
            extend.put(evaluator, new HashMap<>());
            for (Pair<Result.ResultLevel, String> message : messages) {
                LOG.info("Job [{}] Got Message [{}], Level [{}] By Evaluator [{}]",
                        analyzerJobEntity.getJobDefId(), message.getRight(), message.getLeft(), evaluator);
                extend.get(evaluator).put(message.getRight(), message.getLeft().toString());
            }
        }

        Map<String, Object> alertData = new HashMap<>();
        alertData.put(Constants.ANALYZER_REPORT_DATA_BASIC_KEY, basic);
        alertData.put(Constants.ANALYZER_REPORT_DATA_EXTEND_KEY, extend);

        //TODO, override email config in job meta data
        ApplicationEmailService emailService = new ApplicationEmailService(config, Constants.ANALYZER_REPORT_CONFIG_PATH);
        String subject = String.format(Constants.ANALYZER_REPORT_SUBJECT, analyzerJobEntity.getJobDefId());
        AlertEmailContext alertContext = emailService.buildEmailContext(subject);
        emailService.onAlert(alertContext, alertData);
    }

    private String getJobLink(AnalyzerEntity analyzerJobEntity) {
        return "http://"
                + config.getString(Constants.HOST_PATH)
                + ":"
                + config.getInt(Constants.PORT_PATH)
                + "/#/site/"
                + analyzerJobEntity.getSiteId()
                + "/jpm/detail/"
                + analyzerJobEntity.getJobId();
    }
}
