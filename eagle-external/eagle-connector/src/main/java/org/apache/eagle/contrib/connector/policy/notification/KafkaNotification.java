/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */
package org.apache.eagle.contrib.connector.policy.notification;

import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.contrib.connector.policy.common.PolicyConstants;

import java.util.ArrayList;

/**
 * Push alert messages to a KAFKA topic
 * */
public class KafkaNotification implements Notification{
    private String kafkaBroker;
    private String topic;

    public KafkaNotification(){}

    /**
     * @param kafkaBroker is the in format "hostname:port number", eg: localhost:6667
     * */
    public KafkaNotification(String kafkaBroker, String topic){
        this.kafkaBroker = kafkaBroker;
        this.topic = topic;
    }

    public KafkaNotification setKafkaBroker(String kafkaBroker){
        this.kafkaBroker = kafkaBroker;
        return this;
    }
    public String getGetKafkaBroker() {
        return kafkaBroker;
    }

    public KafkaNotification setTopic(String topic){
        this.topic = topic;
        return this;
    }
    public String getTopic(){
        return this.topic;
    }


    @Override
    public String toJSONString() {
        StringBuilder notification = new StringBuilder();
        notification.append("{");

        ArrayList<String> kafkaNotificationEntries = new ArrayList<String>();
        kafkaNotificationEntries.add("\\\"notificationType\\\":\\\"" + PolicyConstants.KAFAKA + "\\\"");

        if(kafkaBroker != null){
            kafkaNotificationEntries.add("\\\"kafka_broker\\\":\\\"" + kafkaBroker + "\\\"");
        }
        if(topic != null){
            kafkaNotificationEntries.add("\\\"topic\\\":\\\"" + topic+ "\\\"");
        }

        String notificationDef = StringUtils.join(kafkaNotificationEntries,",");
        notification.append(notificationDef).append("}");
        return notification.toString();
    }
}
