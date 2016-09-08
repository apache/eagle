/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
/**
 *
 */
package org.apache.eagle.alert.engine.publisher.email;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.PublishConstants;
import org.apache.eagle.alert.utils.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertEmailGenerator {
    private String tplFile;
    private String sender;
    private String recipients;
    private String subject;
    private Map<String, String> properties;

    private ThreadPoolExecutor executorPool;

    private final static Logger LOG = LoggerFactory.getLogger(AlertEmailGenerator.class);

    private final static long MAX_TIMEOUT_MS = 60000;

    public boolean sendAlertEmail(AlertStreamEvent entity) {
        return sendAlertEmail(entity, recipients, null);
    }

    public boolean sendAlertEmail(AlertStreamEvent entity, String recipients) {
        return sendAlertEmail(entity, recipients, null);
    }

    public boolean sendAlertEmail(AlertStreamEvent event, String recipients, String cc) {
        AlertEmailContext email = new AlertEmailContext();
        Map<String, String> alertContext = buildAlertContext(event);
        email.setAlertContext(alertContext);
        email.setVelocityTplFile(tplFile);
        email.setSubject(subject);
        email.setSender(sender);
        email.setRecipients(recipients);
        email.setCc(cc);

        /** asynchronized email sending */
        AlertEmailSender thread = new AlertEmailSender(email, properties);

        if (this.executorPool == null) {
            throw new IllegalStateException("Invoking thread executor pool but it's is not set yet");
        }

        LOG.info("Sending email  in asynchronous to: " + recipients + ", cc: " + cc);
        Future<?> future = this.executorPool.submit(thread);
        Boolean status;
        try {
            future.get(MAX_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            status = true;
            //LOG.info(String.format("Successfully send email to %s", recipients));
        } catch (InterruptedException | ExecutionException e) {
            status = false;
            LOG.error(String.format("Failed to send email to %s, due to:%s", recipients, e), e);
        } catch (TimeoutException e) {
            status = false;
            LOG.error(String.format("Failed to send email to %s due to timeout exception, max timeout: %s ms ", recipients, MAX_TIMEOUT_MS), e);
        }
        return status;
    }

    private Map<String, String> buildAlertContext(AlertStreamEvent event) {
        Map<String, String> alertContext = new HashMap<>();
        alertContext.put(PublishConstants.ALERT_EMAIL_MESSAGE, event.toString());
        alertContext.put(PublishConstants.ALERT_EMAIL_POLICY, event.getPolicyId());
        alertContext.put(PublishConstants.ALERT_EMAIL_TIMESTAMP, DateTimeUtil.millisecondsToHumanDateWithSeconds(event.getCreatedTime()));
        alertContext.put(PublishConstants.ALERT_EMAIL_STREAM, event.getStreamId());
        alertContext.put(PublishConstants.ALERT_EMAIL_CREATOR, event.getCreatedBy());
        return alertContext;
    }

    public String getTplFile() {
        return tplFile;
    }

    public void setTplFile(String tplFile) {
        this.tplFile = tplFile;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getRecipients() {
        return recipients;
    }

    public void setRecipients(String recipients) {
        this.recipients = recipients;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void setExecutorPool(ThreadPoolExecutor executorPool) {
        this.executorPool = executorPool;
    }
}