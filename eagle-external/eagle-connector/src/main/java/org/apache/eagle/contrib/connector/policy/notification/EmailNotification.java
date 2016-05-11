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
 * Notify users using email
 * */
public class EmailNotification implements Notification{
    /**senders email address*/
    private String sender;

    /**recipients email address*/
    private ArrayList<String> recipients;

    /**subject of email*/
    private String subject;

    /**
     * @param sender is sender's email address. eg: abc@example.com
     * */
    public EmailNotification addSender(String sender){
        this.sender = sender;
        return this;
    }

    /**
     * @param recipient is recipient's email address, eg: abc@example.com
     * */
    public EmailNotification addRecipient(String recipient){
        if(recipients == null){
            recipients = new ArrayList<String>();
        }
        recipients.add(recipient);
        return this;
    }

    /**
     * @param subject is the subject of this email
     * */
    public EmailNotification addSubject(String subject){
        this.subject = subject;
        return this;
    }


    @Override
    public String toJSONString(){
        StringBuilder notification = new StringBuilder();
        notification.append("{");

        ArrayList<String> emailNotificationEntries = new ArrayList<String>();
        emailNotificationEntries.add("\\\"notificationType\\\":\\\"" + PolicyConstants.EMAIL + "\\\"");

        if(sender != null) emailNotificationEntries.add("\\\"sender\\\":\\\"" + sender + "\\\"");
        if(recipients != null){
            String str = StringUtils.join(recipients,",");
            emailNotificationEntries.add("\\\"recipients\\\":\\\"" + str + "\\\"");
        }
        if(subject != null){
            emailNotificationEntries.add("\\\"subject\\\":\\\"" + subject + "\\\"");
        }
        String notificationDef = StringUtils.join(emailNotificationEntries,",");

        notification.append(notificationDef).append("}");
        return notification.toString();
    }

}
