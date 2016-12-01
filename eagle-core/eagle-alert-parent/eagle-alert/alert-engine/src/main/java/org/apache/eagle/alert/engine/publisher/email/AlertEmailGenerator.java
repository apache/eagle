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

import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.PublishConstants;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class AlertEmailGenerator {
    private String tplFile;
    private String sender;
    private String recipients;
    private String subject;

    private String serverHost = "localhost";
    private int serverPort = 80;

    private Map<String, Object> properties;

    private ThreadPoolExecutor executorPool;

    private static final Logger LOG = LoggerFactory.getLogger(AlertEmailGenerator.class);

    private static final long MAX_TIMEOUT_MS = 60000;

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

        /** asynchronized email sending. */
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

    /**
     * TODO Support template-based alert message.
     */
    private String getAlertBody(AlertStreamEvent event) {
        if (event.getBody() == null) {
            return String.format("Alert policy \"%s\" was triggered: %s", event.getPolicyId(), generateAlertDataDesc(event));
        } else {
            return event.getBody();
        }
    }

    private String generateAlertDataDesc(AlertStreamEvent event) {
        if (event.getDataMap() == null) {
            return "N/A";
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : event.getDataMap().entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
        }
        return sb.toString();
    }

    private Map<String, String> buildAlertContext(AlertStreamEvent event) {
        Map<String, String> alertContext = new HashMap<>();

        if (event.getContext() != null) {
            for (Map.Entry<String, Object> entry : event.getContext().entrySet()) {
                if (entry.getValue() == null) {
                    alertContext.put(entry.getKey(), "N/A");
                } else {
                    alertContext.put(entry.getKey(), entry.getValue().toString());
                }
            }
        }

        alertContext.put(PublishConstants.ALERT_EMAIL_SUBJECT, event.getSubject());
        alertContext.put(PublishConstants.ALERT_EMAIL_BODY, getAlertBody(event));
        alertContext.put(PublishConstants.ALERT_EMAIL_POLICY_ID, event.getPolicyId());
        alertContext.put(PublishConstants.ALERT_EMAIL_ALERT_ID, event.getAlertId());
        alertContext.put(PublishConstants.ALERT_EMAIL_ALERT_DATA, event.getDataMap().toString());
        alertContext.put(PublishConstants.ALERT_EMAIL_ALERT_DATA_DESC, generateAlertDataDesc(event));
        alertContext.put(PublishConstants.ALERT_EMAIL_TIME, DateTimeUtil.millisecondsToHumanDateWithSeconds(event.getCreatedTime()));
        alertContext.put(PublishConstants.ALERT_EMAIL_STREAM_ID, event.getStreamId());
        alertContext.put(PublishConstants.ALERT_EMAIL_CREATOR, event.getCreatedBy());
        alertContext.put(PublishConstants.ALERT_EMAIL_VERSION, Version.version);


        String rootUrl = this.getServerPort() == 80 ? String.format("http://%s", this.getServerHost())
            : String.format("http://%s:%s", this.getServerHost(), this.getServerPort());
        try {
            alertContext.put(PublishConstants.ALERT_EMAIL_ALERT_DETAIL_URL,
                String.format("%s/#/alert/detail/%s", rootUrl, URIUtil.encodeQuery(event.getAlertId(), "UTF-8")));
            alertContext.put(PublishConstants.ALERT_EMAIL_POLICY_DETAIL_URL,
                String.format("%s/#/policy/detail/%s", rootUrl, URIUtil.encodeQuery(event.getPolicyId(), "UTF-8")));
        } catch (URIException e) {
            LOG.warn(e.getMessage(), e);
            alertContext.put(PublishConstants.ALERT_EMAIL_ALERT_DETAIL_URL,
                String.format("%s/#/alert/detail/%s", rootUrl, event.getAlertId()));
            alertContext.put(PublishConstants.ALERT_EMAIL_POLICY_DETAIL_URL,
                String.format("%s/#/policy/detail/%s", rootUrl, event.getPolicyId()));
        }
        alertContext.put(PublishConstants.ALERT_EMAIL_HOME_URL, rootUrl);
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

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public void setExecutorPool(ThreadPoolExecutor executorPool) {
        this.executorPool = executorPool;
    }

    public String getServerHost() {
        return serverHost;
    }

    public void setServerHost(String serverHost) {
        this.serverHost = serverHost;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }
}