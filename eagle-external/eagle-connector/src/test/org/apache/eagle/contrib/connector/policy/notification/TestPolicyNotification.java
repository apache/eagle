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

import junit.framework.Assert;
import org.junit.Test;


public class TestPolicyNotification {
    @Test
    public void testEmailNotification(){
        EmailNotification notification = new EmailNotification();
        notification.addSender("abc@example.com");
        Assert.assertEquals("{\\\"notificationType\\\":\\\"email\\\",\\\"sender\\\":\\\"abc@example.com\\\"}",notification.toJSONString());
        notification.addRecipient("a@example.com");
        notification.addRecipient("b@example.com");
        notification.addRecipient("c@example.com");
        Assert.assertEquals("{\\\"notificationType\\\":\\\"email\\\",\\\"sender\\\":\\\"abc@example.com\\\",\\\"recipients\\\":\\\"a@example.com,b@example.com,c@example.com\\\"}",notification.toJSONString());
        notification.addSubject("test");
        Assert.assertEquals("{\\\"notificationType\\\":\\\"email\\\",\\\"sender\\\":\\\"abc@example.com\\\",\\\"recipients\\\":\\\"a@example.com,b@example.com,c@example.com\\\",\\\"subject\\\":\\\"test\\\"}",notification.toJSONString());
        System.out.println(notification.toJSONString());
    }

    @Test
    public void testKafkaNotification(){
        KafkaNotification kafkaNotification = new KafkaNotification("localhost:7222","test");
        Assert.assertEquals("{\\\"notificationType\\\":\\\"kafka\\\",\\\"kafka_broker\\\":\\\"localhost:7222\\\",\\\"topic\\\":\\\"test\\\"}",kafkaNotification.toJSONString());
    }

    @Test
    public void testEagleStore(){
        EagleStore eagleStore = new EagleStore();
        Assert.assertEquals("{\\\"notificationType\\\":\\\"EagleStore\\\"}",eagleStore.toJSONString());
    }

    @Test
    public void testNotificaitonDef(){
        NotificationDef notificationDef = new NotificationDef();
        //construct EmailNotificaiton
        EmailNotification emailNotification = new EmailNotification();
        emailNotification.addSender("abc@example.com");
        emailNotification.addRecipient("a@example.com");
        emailNotification.addSubject("test");

        //add email
        notificationDef.addEmailNotification(emailNotification);
        //add kafka
        notificationDef.addKafkaNotification("locohost:7222","test");
        //add eaglestore
        notificationDef.addEgleStore();

        Assert.assertEquals("\"notificationDef\":\"" +
                "[" +
                "{\\\"notificationType\\\":\\\"email\\\",\\\"sender\\\":\\\"abc@example.com\\\",\\\"recipients\\\":\\\"a@example.com\\\",\\\"subject\\\":\\\"test\\\"}," +
                "{\\\"notificationType\\\":\\\"kafka\\\",\\\"kafka_broker\\\":\\\"locohost:7222\\\",\\\"topic\\\":\\\"test\\\"}," +
                "{\\\"notificationType\\\":\\\"EagleStore\\\"}]\"",notificationDef.toJSONEntry());

    }

}