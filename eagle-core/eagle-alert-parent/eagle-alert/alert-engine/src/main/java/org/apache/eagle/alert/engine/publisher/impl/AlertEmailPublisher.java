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

package org.apache.eagle.alert.engine.publisher.impl;

import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.PublishmentType;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertPublishPluginProvider;
import org.apache.eagle.alert.engine.publisher.PublishConstants;
import org.apache.eagle.alert.engine.publisher.email.AlertEmailConstants;
import org.apache.eagle.alert.engine.publisher.email.AlertEmailGenerator;
import org.apache.eagle.alert.engine.publisher.email.AlertEmailGeneratorBuilder;
import com.typesafe.config.Config;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.eagle.alert.service.MetadataServiceClientImpl.*;
import static org.apache.eagle.common.mail.AlertEmailConstants.*;

public class AlertEmailPublisher extends AbstractPublishPlugin implements AlertPublishPluginProvider {

    private static final Logger LOG = LoggerFactory.getLogger(AlertEmailPublisher.class);
    private static final int DEFAULT_THREAD_POOL_CORE_SIZE = 4;
    private static final int DEFAULT_THREAD_POOL_MAX_SIZE = 8;
    private static final long DEFAULT_THREAD_POOL_SHRINK_TIME = 60000L; // 1 minute

    private AlertEmailGenerator emailGenerator;
    private Map<String, Object> emailConfig;

    private transient ThreadPoolExecutor executorPool;
    private String serverHost;
    private int serverPort;
    private Properties mailClientProperties;

    @Override
    @SuppressWarnings("rawtypes")
    public void init(Config config, Publishment publishment, Map conf) throws Exception {
        super.init(config, publishment, conf);
        this.serverHost = config.hasPath(MetadataServiceClientImpl.EAGLE_CORRELATION_SERVICE_HOST)
            ? config.getString(MetadataServiceClientImpl.EAGLE_CORRELATION_SERVICE_HOST) : "localhost";
        this.serverPort = config.hasPath(MetadataServiceClientImpl.EAGLE_CORRELATION_SERVICE_PORT)
            ? config.getInt(MetadataServiceClientImpl.EAGLE_CORRELATION_SERVICE_PORT) : 80;
        this.mailClientProperties = parseMailClientConfig(config);

        executorPool = new ThreadPoolExecutor(DEFAULT_THREAD_POOL_CORE_SIZE, DEFAULT_THREAD_POOL_MAX_SIZE, DEFAULT_THREAD_POOL_SHRINK_TIME, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        LOG.info(" Creating Email Generator... ");
        if (publishment.getProperties() != null) {
            emailConfig = new HashMap<>(publishment.getProperties());
            emailGenerator = createEmailGenerator(emailConfig);
        }
    }

    private Properties parseMailClientConfig(Config config) {
        Properties props = new Properties();
        Config mailConfig = null;
        if (config.hasPath(EAGLE_COORDINATOR_EMAIL_SERVICE)) {
            mailConfig = config.getConfig(EAGLE_COORDINATOR_EMAIL_SERVICE);
        } else if (config.hasPath(EAGLE_APPLICATION_EMAIL_SERVICE)) {
            mailConfig = config.getConfig(EAGLE_APPLICATION_EMAIL_SERVICE);
        }
        String mailSmtpServer = mailConfig.getString(EAGLE_EMAIL_SMTP_SERVER);
        String mailSmtpPort = mailConfig.getString(EAGLE_EMAIL_SMTP_PORT);
        String mailSmtpAuth =  mailConfig.getString(EAGLE_EMAIL_SMTP_AUTH);

        props.put(AlertEmailConstants.CONF_MAIL_HOST, mailSmtpServer);
        props.put(AlertEmailConstants.CONF_MAIL_PORT, mailSmtpPort);
        props.put(AlertEmailConstants.CONF_MAIL_AUTH, mailSmtpAuth);

        if (Boolean.parseBoolean(mailSmtpAuth)) {
            String mailSmtpUsername = mailConfig.getString(EAGLE_EMAIL_SMTP_USERNAME);
            String mailSmtpPassword = mailConfig.getString(EAGLE_EMAIL_SMTP_PASSWORD);
            props.put(AlertEmailConstants.CONF_AUTH_USER, mailSmtpUsername);
            props.put(AlertEmailConstants.CONF_AUTH_PASSWORD, mailSmtpPassword);
        }

        String mailSmtpConn = mailConfig.hasPath(EAGLE_EMAIL_SMTP_CONN) ? mailConfig.getString(EAGLE_EMAIL_SMTP_CONN) : AlertEmailConstants.CONN_PLAINTEXT;
        if (mailSmtpConn.equalsIgnoreCase(AlertEmailConstants.CONN_TLS)) {
            props.put("mail.smtp.starttls.enable", "true");
        }
        if (mailSmtpConn.equalsIgnoreCase(AlertEmailConstants.CONN_SSL)) {
            props.put("mail.smtp.socketFactory.port", "465");
            props.put("mail.smtp.socketFactory.class",
                    "javax.net.ssl.SSLSocketFactory");
        }

        String mailSmtpDebug = mailConfig.hasPath(EAGLE_EMAIL_SMTP_DEBUG) ? mailConfig.getString(EAGLE_EMAIL_SMTP_DEBUG) : "false";
        props.put(AlertEmailConstants.CONF_MAIL_DEBUG, mailSmtpDebug);
        return props;
    }

    @Override
    public void onAlert(AlertStreamEvent event) throws Exception {
        if (emailGenerator == null) {
            LOG.warn("emailGenerator is null due to the incorrect configurations");
            return;
        }
        List<AlertStreamEvent> outputEvents = dedup(event);
        if (outputEvents == null) {
            return;
        }

        boolean isSuccess = true;
        for (AlertStreamEvent outputEvent : outputEvents) {
            if (!emailGenerator.sendAlertEmail(outputEvent)) {
                isSuccess = false;
            }
        }
        PublishStatus status = new PublishStatus();
        if (!isSuccess) {
            status.errorMessage = "Failed to send email";
            status.successful = false;
        } else {
            status.errorMessage = "";
            status.successful = true;
        }
        this.status = status;
    }

    @Override
    public void update(String dedupIntervalMin, Map<String, Object> pluginProperties) {
        super.update(dedupIntervalMin, pluginProperties);

        if (pluginProperties != null && !emailConfig.equals(pluginProperties)) {
            emailConfig = new HashMap<>(pluginProperties);
            emailGenerator = createEmailGenerator(pluginProperties);
        }
    }

    @Override
    public void close() {
        this.executorPool.shutdown();
    }

    private AlertEmailGenerator createEmailGenerator(Map<String, Object> notificationConfig) {
        String tplFileName = (String) notificationConfig.get(PublishConstants.TEMPLATE);
        if (tplFileName == null || tplFileName.equals("")) {
            // tplFileName = "ALERT_DEFAULT_TEMPLATE.vm";
            // tplFileName = "ALERT_LIGHT_TEMPLATE.vm";
            tplFileName = "ALERT_INLINED_TEMPLATE.vm";
        }
        String subject = (String) notificationConfig.get(PublishConstants.SUBJECT);
        if (subject == null) {
            subject = "No subject";
        }
        String sender = (String) notificationConfig.get(PublishConstants.SENDER);
        String recipients = (String) notificationConfig.get(PublishConstants.RECIPIENTS);
        if (sender == null || recipients == null) {
            LOG.warn("email sender or recipients is null");
            return null;
        }

        AlertEmailGenerator gen = AlertEmailGeneratorBuilder.newBuilder()
            .withMailProps(this.mailClientProperties)
            .withSubject(subject)
            .withSender(sender)
            .withRecipients(recipients)
            .withTplFile(tplFileName)
            .withExecutorPool(this.executorPool)
            .withServerHost(this.serverHost)
            .withServerPort(this.serverPort)
            .build();
        return gen;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getClass().getCanonicalName()).toHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof AlertEmailPublisher)) {
            return false;
        }
        return true;
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    public PublishmentType getPluginType() {
        return new PublishmentType.Builder()
                .name("Email")
                .type(AlertEmailPublisher.class)
                .description("Email alert publisher")
                .field("subject")
                .field("sender")
                .field("recipients")
                .build();
    }
}
