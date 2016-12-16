/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.common.mail;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.Version;
import org.apache.velocity.VelocityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AlertEmailSender {
    private static final Logger LOG = LoggerFactory.getLogger(AlertEmailSender.class);

    private List<Map<String, Object>> alertContexts = new ArrayList<>();
    private String origin;

    private AlertEmailContext mailProps;
    private Properties serverProps;

    private static final int MAX_RETRY_COUNT = 3;

    /**
     * Derived class may have some additional context properties to add.
     * @param context velocity context
     */
    public void addAlertContext(Map<String, Object> context) {
        alertContexts.add(context);
    }

    public AlertEmailSender(AlertEmailContext mailProps, Properties serverProps) {
        this.mailProps = mailProps;
        this.serverProps = serverProps;
        String tmp = ManagementFactory.getRuntimeMXBean().getName();
        this.origin = tmp.split("@")[1] + "(pid:" + tmp.split("@")[0] + ")";
        LOG.info("Initialized email sender: origin is :{}, recipient of the email: {}, velocity TPL file: {}",
            origin, mailProps.getRecipients(), mailProps.getVelocityTplFile());
    }

    public boolean send() {
        int count = 0;
        boolean success = false;
        while (count++ < MAX_RETRY_COUNT && !success) {
            LOG.info("Sending email, tried: " + count + ", max: " + MAX_RETRY_COUNT);
            try {
                final EagleMailClient client;
                if (serverProps != null) {
                    client = new EagleMailClient(serverProps);
                } else {
                    client = new EagleMailClient();
                }

                final VelocityContext context = new VelocityContext();
                generateCommonContext(context);
                LOG.info("After calling generateCommonContext...");

                if (mailProps.getRecipients() == null || mailProps.getRecipients().equals("")) {
                    LOG.error("Recipients is null, skip sending emails ");
                    return success;
                }

                success = client.send(mailProps.getSender(),
                    mailProps.getRecipients(),
                    mailProps.getCc(),
                    mailProps.getSubject(),
                    mailProps.getVelocityTplFile(),
                    context,
                    null);
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
            LOG.info("Successfully send email with subject {}", mailProps.getSubject());
        } else {
            LOG.warn("Fail sending email after tries {} times, subject: %s", MAX_RETRY_COUNT, mailProps.getSubject());
        }
        return success;
    }

    private void generateCommonContext(VelocityContext context) {
        context.put(AlertEmailConstants.ALERT_EMAIL_TIME_PROPERTY, DateTimeUtil.millisecondsToHumanDateWithSeconds(System.currentTimeMillis()));
        context.put(AlertEmailConstants.ALERT_EMAIL_COUNT_PROPERTY, alertContexts.size());
        context.put(AlertEmailConstants.ALERT_EMAIL_ALERTLIST_PROPERTY, alertContexts);
        context.put(AlertEmailConstants.ALERT_EMAIL_ORIGIN_PROPERTY, origin);
        context.put(AlertEmailConstants.VERSION, Version.version);
    }
}