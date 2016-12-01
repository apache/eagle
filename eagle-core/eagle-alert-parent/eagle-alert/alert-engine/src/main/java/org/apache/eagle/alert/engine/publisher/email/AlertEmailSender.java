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
package org.apache.eagle.alert.engine.publisher.email;

import org.apache.eagle.alert.engine.publisher.PublishConstants;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.velocity.VelocityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AlertEmailSender implements Runnable {

    protected final List<Map<String, String>> alertContexts = new ArrayList<Map<String, String>>();
    protected final String configFileName;
    protected final String subject;
    protected final String sender;
    protected final String recipients;
    protected final String cc;
    protected final String origin;
    protected boolean sentSuccessfully = false;

    private static final Logger LOG = LoggerFactory.getLogger(AlertEmailSender.class);
    private static final int MAX_RETRY_COUNT = 3;

    private Properties mailProps;
    private String threadName;

    /**
     * Derived class may have some additional context properties to add.
     *
     * @param context velocity context
     * @param env     environment
     */
    protected void additionalContext(VelocityContext context, String env) {
        // By default there's no additional context added
    }

    public AlertEmailSender(AlertEmailContext alertEmail) {
        this.recipients = alertEmail.getRecipients();
        this.configFileName = alertEmail.getVelocityTplFile();
        this.subject = alertEmail.getSubject();
        this.sender = alertEmail.getSender();
        this.cc = alertEmail.getCc();

        this.alertContexts.add(alertEmail.getAlertContext());
        String tmp = ManagementFactory.getRuntimeMXBean().getName();
        this.origin = tmp.split("@")[1] + "(pid:" + tmp.split("@")[0] + ")";
        threadName = Thread.currentThread().getName();
        LOG.info("Initialized " + threadName + ": origin is : " + this.origin + ", recipient of the email: " + this.recipients + ", velocity TPL file: " + this.configFileName);
    }

    public AlertEmailSender(AlertEmailContext alertEmail, Properties mailProps) {
        this(alertEmail);
        this.mailProps = mailProps;
    }

    @Override
    public void run() {
        int count = 0;
        boolean success = false;
        while (count++ < MAX_RETRY_COUNT && !success) {
            LOG.info("Sending email, tried: " + count + ", max: " + MAX_RETRY_COUNT);
            try {
                final EagleMailClient client;
                if (mailProps != null) {
                    client = new EagleMailClient(mailProps);
                } else {
                    client = new EagleMailClient();
                }

                final VelocityContext context = new VelocityContext();
                generateCommonContext(context);
                LOG.info("After calling generateCommonContext...");

                if (recipients == null || recipients.equals("")) {
                    LOG.error("Recipients is null, skip sending emails ");
                    return;
                }
                String title = subject;

                success = client.send(sender, recipients, cc, title, configFileName, context, null);
                LOG.info("Success of sending email: " + success);
                if (!success && count < MAX_RETRY_COUNT) {
                    LOG.info("Sleep for a while before retrying");
                    Thread.sleep(10 * 1000);
                }
            } catch (Exception e) {
                LOG.warn("Sending mail exception", e);
            }
        }
        if (success) {
            sentSuccessfully = true;
            LOG.info(String.format("Successfully send email, thread: %s", threadName));
        } else {
            LOG.warn(String.format("Fail sending email after tries %s times, thread: %s", MAX_RETRY_COUNT, threadName));
        }
    }

    private void generateCommonContext(VelocityContext context) {
        context.put(PublishConstants.ALERT_EMAIL_TIME_PROPERTY, DateTimeUtil.millisecondsToHumanDateWithSeconds(System.currentTimeMillis()));
        context.put(PublishConstants.ALERT_EMAIL_COUNT_PROPERTY, alertContexts.size());
        context.put(PublishConstants.ALERT_EMAIL_ALERTLIST_PROPERTY, alertContexts);
        context.put(PublishConstants.ALERT_EMAIL_ORIGIN_PROPERTY, origin);
    }

}