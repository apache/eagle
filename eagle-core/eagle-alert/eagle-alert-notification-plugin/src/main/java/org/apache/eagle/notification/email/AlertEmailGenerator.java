/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package org.apache.eagle.notification.email;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.apache.eagle.common.metric.AlertContext;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import com.typesafe.config.ConfigObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertEmailGenerator{
    private String tplFile;
    private String sender;
    private String recipients;
    private String subject;
    private ConfigObject eagleProps;

    private ThreadPoolExecutor executorPool;

    private final static Logger LOG = LoggerFactory.getLogger(AlertEmailGenerator.class);

    private final static long MAX_TIMEOUT_MS = 60000;

    private final static String EVENT_FIELDS_SPLITTER = ",";

    public boolean sendAlertEmail(AlertAPIEntity entity) {
        return sendAlertEmail(entity, recipients, null);
    }

    public boolean sendAlertEmail(AlertAPIEntity entity, String recipients) {
        return sendAlertEmail(entity, recipients, null);
    }

    public boolean sendAlertEmail(AlertAPIEntity entity, String recipients, String cc) {
        boolean sentSuccessfully = false;
        AlertEmailContext email = new AlertEmailContext();

        AlertEmailComponent component = new AlertEmailComponent();
        AlertContext context = AlertContext.fromJsonString(entity.getAlertContext());
        component.setAlertContext(context);
        AlertEmailComponent eventComponent = getEventComponent(context);
        List<AlertEmailComponent> components = new ArrayList<AlertEmailComponent>();
        components.add(component);
        components.add(eventComponent);
        email.setComponents(components);

        if (context.getProperty(Constants.SUBJECT) != null) {
            email.setSubject(context.getProperty(Constants.SUBJECT));
        }
        else {
            email.setSubject(subject);
        }

        email.setVelocityTplFile(tplFile);
        email.setRecipients(recipients);
        email.setCc(cc);
        email.setSender(sender);

        /** asynchronized email sending */
        @SuppressWarnings("rawtypes")
        AlertEmailSender thread = new AlertEmailSender(email, eagleProps);

        if(this.executorPool == null) throw new IllegalStateException("Invoking thread executor pool but it's is not set yet");

        LOG.info("Sending email in asynchronous to: "+ recipients +", cc: " + cc);
        Future future = this.executorPool.submit(thread);
        try {
            future.get(MAX_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            sentSuccessfully = true;
            LOG.info(String.format("Successfully send email to %s", recipients));
        } catch (InterruptedException | ExecutionException  e) {
            sentSuccessfully = false;
            LOG.error(String.format("Failed to send email to %s, due to:%s",recipients,e),e);
        } catch (TimeoutException e) {
            sentSuccessfully = false;
            LOG.error(String.format("Failed to send email to %s due to timeout exception, max timeout: %s ms ",recipients, MAX_TIMEOUT_MS),e);
        }
        return sentSuccessfully;
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

    public ConfigObject getEagleProps() {
        return eagleProps;
    }

    public void setEagleProps(ConfigObject eagleProps) {
        this.eagleProps = eagleProps;
    }

    public void setExecutorPool(ThreadPoolExecutor executorPool) {
        this.executorPool = executorPool;
    }

    private AlertEmailComponent getEventComponent(AlertContext context) {
        AlertContext eventFieldsContext = new AlertContext();
        String eventFields = context.getProperty(Constants.ALERT_EVENT_FIELDS);
        String[] fields = eventFields.split(EVENT_FIELDS_SPLITTER);

        for (String key : fields) {
            eventFieldsContext.addProperty(key, context.getProperty(key));
        }

        AlertEmailComponent component = new AlertEmailComponent();
        component.setAlertContext(eventFieldsContext);

        return component;
    }
}