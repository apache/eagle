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

import java.util.ArrayList;

/**
 * NotificationDef refers to json entry "notificationDef" in policy's JSON format,
 * it contains all the notifications setting
 * */
public class NotificationDef {
    private ArrayList<Notification> notificationList;

    public NotificationDef(){
        notificationList = new ArrayList<>();
        // by default we use eagle store notification
        notificationList.add(new EagleStore());
    }

    public NotificationDef addKafkaNotification(String kafkaBroker, String topic){
        KafkaNotification kafkaNotification = new KafkaNotification();
        kafkaNotification.setKafkaBroker(kafkaBroker)
                .setTopic(topic);
        notificationList.add(kafkaNotification);
        return this;
    }

    public NotificationDef addEmailNotification(EmailNotification emailNotification){
        notificationList.add(emailNotification);
        return this;
    }

    public NotificationDef addEgleStore(){
        notificationList.add(new EagleStore());
        return this;
    }


    /**
     *@return jsonEntry  "notificationDef"  in policy's JSON format
     * */
    public String toJSONEntry(){
        StringBuilder jsonEntry = new StringBuilder();
        jsonEntry.append("\"notificationDef\":\"[");

        if(notificationList.size() != 0){
            jsonEntry.append(notificationList.get(0).toJSONString());
            for(int i = 1; i < notificationList.size(); i++){
                jsonEntry.append(",").append(notificationList.get(i).toJSONString());
            }
        }

        jsonEntry.append("]\"");
        return jsonEntry.toString();
    }


}
