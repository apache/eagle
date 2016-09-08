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

import com.typesafe.config.Config;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.PublishConstants;
import org.apache.eagle.alert.engine.publisher.email.AlertEmailGenerator;
import org.apache.eagle.alert.engine.publisher.email.AlertEmailGeneratorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AlertEmailPublisher extends AbstractPublishPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(AlertEmailPublisher.class);
    private final static int DEFAULT_THREAD_POOL_CORE_SIZE = 4;
    private final static int DEFAULT_THREAD_POOL_MAX_SIZE = 8;
    private final static long DEFAULT_THREAD_POOL_SHRINK_TIME = 60000L; // 1 minute

    private AlertEmailGenerator emailGenerator;
    private Map<String, String> emailConfig;

    private transient ThreadPoolExecutor executorPool;

    @Override
    @SuppressWarnings("rawtypes")
    public void init(Config config, Publishment publishment, Map conf) throws Exception {
        super.init(config, publishment, conf);
        executorPool = new ThreadPoolExecutor(DEFAULT_THREAD_POOL_CORE_SIZE, DEFAULT_THREAD_POOL_MAX_SIZE, DEFAULT_THREAD_POOL_SHRINK_TIME, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        LOG.info(" Creating Email Generator... ");
        if (publishment.getProperties() != null) {
            emailConfig = new HashMap<>(publishment.getProperties());
            emailGenerator = createEmailGenerator(emailConfig);
        }
    }

    @Override
    public void onAlert(AlertStreamEvent event) throws Exception {
        if(emailGenerator == null) {
            LOG.warn("emailGenerator is null due to the incorrect configurations");
            return;
        }
        List<AlertStreamEvent> outputEvents = dedup(event);
        if(outputEvents == null) {
            return;
        }
        
        boolean isSuccess = true;
        for (AlertStreamEvent outputEvent : outputEvents) {
        	if (!emailGenerator.sendAlertEmail(outputEvent)) {
        		isSuccess = false;
        	}
        }
        PublishStatus status = new PublishStatus();
        if(!isSuccess) {
            status.errorMessage = "Failed to send email";
            status.successful = false;
        } else {
            status.errorMessage = "";
            status.successful = true;
        }
        this.status = status;
    }

    @Override
    public void update(String dedupIntervalMin, Map<String, String> pluginProperties) {
        super.update(dedupIntervalMin, pluginProperties);

        if (pluginProperties != null && ! emailConfig.equals(pluginProperties)) {
            emailConfig = new HashMap<>(pluginProperties);
            emailGenerator = createEmailGenerator(pluginProperties);
        }
    }

    @Override
    public void close() {
        this.executorPool.shutdown();
    }

    /**
     * @param notificationConfig
     * @return
     */
    private AlertEmailGenerator createEmailGenerator(Map<String, String> notificationConfig) {
        String tplFileName = notificationConfig.get(PublishConstants.TEMPLATE);
        if (tplFileName == null || tplFileName.equals("")) {
            tplFileName = "ALERT_DEFAULT.vm";
        }
        String subject = notificationConfig.get(PublishConstants.SUBJECT);
        if (subject == null) {
            subject = "No subject";
        }
        String sender = notificationConfig.get(PublishConstants.SENDER);
        String recipients = notificationConfig.get(PublishConstants.RECIPIENTS);
        if(sender == null || recipients == null) {
            LOG.warn("email sender or recipients is null");
            return null;
        }
        AlertEmailGenerator gen = AlertEmailGeneratorBuilder.newBuilder().
                withMailProps(notificationConfig).
                withSubject(subject).
                withSender(sender).
                withRecipients(recipients).
                withTplFile(tplFileName).
                withExecutorPool(this.executorPool).build();
        return gen;
    }

    @Override
    public int hashCode(){
        return new HashCodeBuilder().append(getClass().getCanonicalName()).toHashCode();
    }

    @Override
    public boolean equals(Object o){
        if(o == this)
            return true;
        if(!(o instanceof AlertEmailPublisher))
            return false;
        return true;
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
