/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.service;

import com.codahale.metrics.health.HealthCheck;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.engine.publisher.email.EagleMailClient;
import org.apache.eagle.app.service.impl.ApplicationHealthCheckEmailPublisher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import static org.mockito.Mockito.*;

import java.util.HashMap;

/**
 * @Since 11/24/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ApplicationHealthCheckEmailPublisher.class})
public class TestApplicationHealthCheckEmailPublisher {

    @Test
    public void testOnUnHealthApplication() throws Exception {
        Config config = ConfigFactory.parseMap(new HashMap<String, String>() {
            {
                put("mail.smtp.recipients", "recipient@eagle.com");
                put("mail.smtp.sender", "sender@eagle.com");
                put("mail.smtp.subject", "test_subject");
                put("mail.smtp.cc", "cc@eagle.com");
                put("mail.smtp.template", "test_template");
                put("mail.smtp.host", "localhost");
                put("mail.smtp.port", "25");
                put("host", "localhost");
                put("port", "9090");
            }
        });
        ApplicationHealthCheckEmailPublisher publisher = new ApplicationHealthCheckEmailPublisher(config);
        EagleMailClient client = mock(EagleMailClient.class);
        PowerMockito.whenNew(EagleMailClient.class).withAnyArguments().thenReturn(client);
        when(client.send(anyString(), anyString(), anyString(), anyString(), anyString(), anyObject(), anyObject())).thenReturn(false, true);
        publisher.onUnHealthApplication("appId", new HashMap<String, HealthCheck.Result>(){{
            put("appId", HealthCheck.Result.healthy());
        }});
        verify(client, times(2)).send(anyString(), anyString(), anyString(), anyString(), anyString(), anyObject(), anyObject());
    }
}
