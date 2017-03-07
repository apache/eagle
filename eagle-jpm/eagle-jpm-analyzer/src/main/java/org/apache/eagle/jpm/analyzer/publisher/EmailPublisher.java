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
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.engine.publisher.PublishConstants;
import org.apache.eagle.app.service.ApplicationEmailService;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.mail.AlertEmailConstants;
import org.apache.eagle.common.mail.AlertEmailContext;
import org.apache.eagle.jpm.analyzer.meta.model.AnalyzerEntity;
import org.apache.eagle.jpm.analyzer.meta.model.UserEmailEntity;
import org.apache.eagle.jpm.analyzer.mr.sla.SLAJobEvaluator;
import org.apache.eagle.jpm.analyzer.util.Constants;
import org.apache.eagle.jpm.analyzer.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EmailPublisher implements Publisher, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(EmailPublisher.class);

    private Config config;
    private List<Class<?>> publishResult4Evaluators = new ArrayList<>();

    public EmailPublisher(Config config) {
        this.config = config;
        //TODO
        publishResult4Evaluators.add(SLAJobEvaluator.class);
    }

    @Override
    //will refactor, just work now
    public void publish(AnalyzerEntity analyzerJobEntity, Result result) {
        if (!config.hasPath(Constants.ANALYZER_REPORT_CONFIG_PATH)) {
            LOG.warn("no email configuration, skip send email");
            return;
        }

        Map<String, List<Result.ProcessorResult>> extend = new HashMap<>();
        publishResult4Evaluators
                .stream()
                .filter(item -> result.getAlertMessages().containsKey(item.getName()))
                .forEach(item -> extend.put(item.getName(), result.getAlertMessages().get(item.getName()))
                );

        if (extend.size() == 0) {
            return;
        }

        LOG.info("EmailPublisher gets job {}", analyzerJobEntity.getJobDefId());

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
        double progress = analyzerJobEntity.getCurrentState().equalsIgnoreCase(org.apache.eagle.jpm.util.Constants.JobState.RUNNING.toString()) ? analyzerJobEntity.getProgress() : 100;
        basic.put("progress", progress + "%");
        basic.put("detail", getJobLink(analyzerJobEntity));

        Map<String, Object> alertData = new HashMap<>();
        for (String evaluator : extend.keySet()) {
            for (Result.ProcessorResult message : extend.get(evaluator)) {
                setAlertLevel(alertData, message.getResultLevel());
                LOG.info("Job [{}] Got Message [{}], Level [{}] By Evaluator [{}]",
                        analyzerJobEntity.getJobDefId(), message.getMessage(), message.getResultLevel(), evaluator);
            }
        }

        alertData.put(Constants.ANALYZER_REPORT_DATA_BASIC_KEY, basic);
        alertData.put(Constants.ANALYZER_REPORT_DATA_EXTEND_KEY, extend);
        Config cloneConfig = ConfigFactory.empty().withFallback(config);
        if (analyzerJobEntity.getUserId() != null) {
            List<UserEmailEntity> users = Utils.getUserMail(config, analyzerJobEntity.getSiteId(), analyzerJobEntity.getUserId());
            if (users != null && users.size() > 0) {
                Map<String, String> additionalConfig = new HashMap<>();
                additionalConfig.put(Constants.ANALYZER_REPORT_CONFIG_PATH + "." + AlertEmailConstants.RECIPIENTS, users.get(0).getMailAddress());
                cloneConfig = ConfigFactory.parseMap(additionalConfig).withFallback(cloneConfig);
            }
        }
        ApplicationEmailService emailService = new ApplicationEmailService(cloneConfig, Constants.ANALYZER_REPORT_CONFIG_PATH);
        String subject = String.format(Constants.ANALYZER_REPORT_SUBJECT, analyzerJobEntity.getJobDefId());
        alertData.put(PublishConstants.ALERT_EMAIL_SUBJECT, subject);
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

    private void setAlertLevel(Map<String, Object> alertData, Result.ResultLevel level) {
        if (!alertData.containsKey(PublishConstants.ALERT_EMAIL_ALERT_SEVERITY)) {
            alertData.put(PublishConstants.ALERT_EMAIL_ALERT_SEVERITY, Result.ResultLevel.INFO.toString());
        }

        if (level.equals(Result.ResultLevel.CRITICAL)) {
            alertData.put(PublishConstants.ALERT_EMAIL_ALERT_SEVERITY, level.toString());
        }

        if (level.equals(Result.ResultLevel.WARNING)
                && !alertData.get(PublishConstants.ALERT_EMAIL_ALERT_SEVERITY).equals(Result.ResultLevel.CRITICAL.toString())) {
            alertData.put(PublishConstants.ALERT_EMAIL_ALERT_SEVERITY, level.toString());
        }
    }
}
