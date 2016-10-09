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
import com.ullink.slack.simpleslackapi.SlackAttachment;
import com.ullink.slack.simpleslackapi.SlackChannel;
import com.ullink.slack.simpleslackapi.SlackSession;
import com.ullink.slack.simpleslackapi.impl.SlackSessionFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.PublishConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @since Sep 14, 2016.
 */
public class AlertSlackPublisher extends AbstractPublishPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(AlertSlackPublisher.class);

    private SlackSession session;
    private String slackChannels;
    private String severitys;

    @Override
    public void init(Config config, Publishment publishment, Map conf) throws Exception {
        super.init(config, publishment, conf);

        if (publishment.getProperties() != null) {
            Map<String, String> slackConfig = new HashMap<>(publishment.getProperties());
            final String token = slackConfig.get(PublishConstants.TOKEN).trim();
            slackChannels = slackConfig.get(PublishConstants.CHANNELS).trim();
            severitys = slackConfig.get(PublishConstants.SEVERITYS).trim();

            if (StringUtils.isNotEmpty(token)) {
                LOG.debug(" Creating Slack Session... ");
                session = SlackSessionFactory.createWebSocketSlackSession(token);
                session.connect();
            }
        }

    }

    @Override
    public void onAlert(AlertStreamEvent event) throws Exception {
        if (session == null) {
            LOG.warn("Slack session is null due to incorrect configurations!");
            return;
        }
        List<AlertStreamEvent> outputEvents = dedup(event);
        if (outputEvents == null) {
            return;
        }

        PublishStatus status = new PublishStatus();
        for (AlertStreamEvent outputEvent: outputEvents) {
            String message = "";
            String severity = "";
            String color = "";
            // only user defined severity level alert will send to Slack;
            boolean publishToSlack = false;

            StreamDefinition streamDefinition = outputEvent.getSchema();
            for (int i = 0; i < outputEvent.getData().length; i++) {
                if (i > streamDefinition.getColumns().size()) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("output column does not found for event data, this indicate code error!");
                    }
                    continue;
                }
                String colName = streamDefinition.getColumns().get(i).getName();
                if (colName.equalsIgnoreCase("severity")) {
                    severity = outputEvent.getData()[i].toString();
                    publishToSlack = severitys.contains(severity);
                }
                if (colName.equalsIgnoreCase("message")) {
                    message = outputEvent.getData()[i].toString();
                }
            }

            if (publishToSlack) {
                try {
                    // get hex color code from severity
                    switch (severity) {
                        case "CRITICAL":
                            color = "#dd3333"; //red
                            break;
                        case "WARNING":
                            color = "#ffc04c"; //yellow
                            break;
                        default:
                            color = "#439FE0"; //blue
                            break;
                    }

                    // here to be generic, only publish message like "CRITICAL  port-1 is down" to Slack
                    String messageToSlack = String.format("%s %s", severity, message);
                    SlackAttachment attachment = new SlackAttachment();
                    attachment.setColor(color);
                    attachment.setText(messageToSlack);

                    for (String slackChannel: slackChannels.split(",")) {
                        sendMessageToAChannel(session, slackChannel, null, attachment);
                    }
                } catch (Exception e) {
                    status.successful = false;
                    status.errorMessage = String.format("Failed to send message to slack channel %s, due to:%s", slackChannels, e);
                    LOG.error(status.errorMessage, e);
                }
            }
        }

    }

    @Override
    public void close() {
        try {
            session.disconnect();
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    private void sendMessageToAChannel(SlackSession session, String channelName, String message, SlackAttachment attachment) {
        //get a channel
        SlackChannel channel = session.findChannelByName(channelName);
        session.sendMessage(channel, message, attachment);
    }
}
