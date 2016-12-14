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

package org.apache.eagle.app.service;

import com.typesafe.config.Config;
import org.apache.eagle.common.mail.AbstractEmailService;
import org.apache.eagle.common.mail.AlertEmailConstants;
import org.apache.eagle.common.mail.AlertEmailContext;
import org.apache.eagle.common.mail.AlertEmailSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class ApplicationEmailService extends AbstractEmailService {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationEmailService.class);
    private String appConfPath;
    private Config config;

    public ApplicationEmailService(Config config, String appConfPath) {
        super(config);
        this.appConfPath = appConfPath;
        this.config = config;
    }

    public boolean onAlert(Map<String, Object> alertData) {
        return super.onAlert(buildEmailContext(), alertData);
    }

    private String buildDefaultSender() {
        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            if (!hostname.endsWith(".com")) {
                //avoid invalid host exception
                hostname += ".com";
            }
        } catch (UnknownHostException e) {
            LOG.warn("UnknownHostException when get local hostname");
        }
        return System.getProperty("user.name") + "@" + hostname;
    }

    public AlertEmailContext buildEmailContext() {
         return buildEmailContext(null);
    }

    public AlertEmailContext buildEmailContext(String mailSubject) {
        AlertEmailContext mailProps = new AlertEmailContext();
        Config appConfig = config.getConfig(appConfPath);
        String tplFileName = appConfig.getString(AlertEmailConstants.TEMPLATE);
        if (tplFileName == null || tplFileName.equals("")) {
            tplFileName = "ALERT_INLINED_TEMPLATE.vm";
        }
        String subject;
        if (mailSubject != null) {
            subject = mailSubject;
        } else {
            subject = appConfig.getString(AlertEmailConstants.SUBJECT);
        }
        String sender;
        if (!appConfig.hasPath(AlertEmailConstants.SENDER)) {
            sender = buildDefaultSender();
        } else {
            sender = appConfig.getString(AlertEmailConstants.SENDER);
        }
        if (!appConfig.hasPath(AlertEmailConstants.RECIPIENTS)) {
            throw new IllegalArgumentException("email sender or recipients is null");
        }
        String recipients = appConfig.getString(AlertEmailConstants.RECIPIENTS);
        mailProps.setSubject(subject);
        mailProps.setRecipients(recipients);
        mailProps.setSender(sender);
        mailProps.setVelocityTplFile(tplFileName);
        return mailProps;
    }
    
    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
