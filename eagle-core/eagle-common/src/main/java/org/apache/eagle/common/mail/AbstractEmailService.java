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

package org.apache.eagle.common.mail;

import com.typesafe.config.Config;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Properties;

import static org.apache.eagle.common.mail.AlertEmailConstants.*;

public abstract class AbstractEmailService {

    private Properties serverProps;

    public AbstractEmailService(Config config) {
        serverProps = parseMailClientConfig(config);
    }

    protected abstract Logger getLogger();

    private Properties parseMailClientConfig(Config config) {
        Properties props = new Properties();
        String mailSmtpServer = config.getString(EAGLE_EMAIL_SMTP_SERVER);
        String mailSmtpPort = config.getString(EAGLE_EMAIL_SMTP_PORT);
        String mailSmtpAuth =  config.getString(EAGLE_EMAIL_SMTP_AUTH);

        props.put(AlertEmailConstants.CONF_MAIL_HOST, mailSmtpServer);
        props.put(AlertEmailConstants.CONF_MAIL_PORT, mailSmtpPort);
        props.put(AlertEmailConstants.CONF_MAIL_AUTH, mailSmtpAuth);

        if (Boolean.parseBoolean(mailSmtpAuth)) {
            String mailSmtpUsername = config.getString(EAGLE_EMAIL_SMTP_USERNAME);
            String mailSmtpPassword = config.getString(EAGLE_EMAIL_SMTP_PASSWORD);
            props.put(AlertEmailConstants.CONF_AUTH_USER, mailSmtpUsername);
            props.put(AlertEmailConstants.CONF_AUTH_PASSWORD, mailSmtpPassword);
        }

        String mailSmtpConn = config.hasPath(EAGLE_EMAIL_SMTP_CONN) ? config.getString(EAGLE_EMAIL_SMTP_CONN) : AlertEmailConstants.CONN_PLAINTEXT;
        String mailSmtpDebug = config.hasPath(EAGLE_EMAIL_SMTP_DEBUG) ? config.getString(EAGLE_EMAIL_SMTP_DEBUG) : "false";
        if (mailSmtpConn.equalsIgnoreCase(AlertEmailConstants.CONN_TLS)) {
            props.put("mail.smtp.starttls.enable", "true");
        }
        if (mailSmtpConn.equalsIgnoreCase(AlertEmailConstants.CONN_SSL)) {
            props.put("mail.smtp.socketFactory.port", "465");
            props.put("mail.smtp.socketFactory.class",
                "javax.net.ssl.SSLSocketFactory");
        }
        props.put(AlertEmailConstants.CONF_MAIL_DEBUG, mailSmtpDebug);
        return props;
    }

    public boolean onAlert(AlertEmailContext mailContext, Map<String, Object> alertData) {
        /** synchronized email sending. */
        if (alertData == null || alertData.isEmpty()) {
            getLogger().warn("alertData for {} is empty");
            return false;
        }
        AlertEmailSender mailSender = new AlertEmailSender(mailContext, serverProps);
        mailSender.addAlertContext(alertData);
        getLogger().info("Sending email in synchronous mode to: {} cc: {}", mailContext.getRecipients(), mailContext.getCc());
        return mailSender.send();
    }

}
