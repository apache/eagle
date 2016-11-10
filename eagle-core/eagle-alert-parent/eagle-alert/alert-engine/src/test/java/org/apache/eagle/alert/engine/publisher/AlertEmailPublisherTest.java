/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.publisher;

import com.dumbster.smtp.SimpleSmtpServer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.publisher.email.AlertEmailConstants;
import org.apache.eagle.alert.engine.publisher.impl.AlertEmailPublisher;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AlertEmailPublisherTest {
    private static final String EMAIL_PUBLISHER_TEST_POLICY = "EMAIL_PUBLISHER_TEST_POLICY";
    private static final Logger LOG = LoggerFactory.getLogger(AlertEmailPublisherTest.class);
    private static final int SMTP_PORT = 5025;
    private Config config;
    private SimpleSmtpServer server;

    @Before
    public void setUp(){
        config = ConfigFactory.load();
        server = SimpleSmtpServer.start(SMTP_PORT);
    }

    @After
    public void clear(){
        if(server!=null) {
            server.stop();
        }
    }

    @Test
    public void testAlertEmailPublisher() throws Exception {
        AlertEmailPublisher publisher = new AlertEmailPublisher();
        Map<String, Object> properties = new HashMap<>();
        properties.put(PublishConstants.SUBJECT,"EMAIL_PUBLISHER_TEST_POLICY_ALERT");
        properties.put(PublishConstants.SENDER,"eagle@localhost");
        properties.put(PublishConstants.RECIPIENTS,"somebody@localhost");
        properties.put(AlertEmailConstants.CONF_MAIL_HOST,"localhost");
        properties.put(AlertEmailConstants.CONF_MAIL_PORT,String.valueOf(SMTP_PORT));
        Publishment publishment = new Publishment();
        publishment.setName("testEmailPublishment");
        publishment.setType(org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher.class.getName());
        publishment.setPolicyIds(Collections.singletonList(EMAIL_PUBLISHER_TEST_POLICY));
        publishment.setDedupIntervalMin("PT0M");
        publishment.setSerializer(org.apache.eagle.alert.engine.publisher.impl.JsonEventSerializer.class.getName());
        publishment.setProperties(properties);
        Map<String, String> conf = new HashMap<>();
        publisher.init(config, publishment,conf);
        publisher.onAlert(AlertPublisherTestHelper.mockEvent(EMAIL_PUBLISHER_TEST_POLICY));
        Assert.assertEquals(1,server.getReceivedEmailSize());
        Assert.assertTrue(server.getReceivedEmail().hasNext());
        LOG.info("EMAIL:\n {}", server.getReceivedEmail().next());
    }
}
