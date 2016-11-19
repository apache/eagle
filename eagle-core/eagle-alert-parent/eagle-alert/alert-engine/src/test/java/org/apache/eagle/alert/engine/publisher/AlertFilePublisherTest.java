/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *  <p/>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p/>
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.publisher;

import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.impl.AlertFilePublisher;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class AlertFilePublisherTest {
    private static final String TEST_POLICY_ID = "testPolicy";

    @Test
    public void testAlertFilePublisher() throws Exception {
        Map<String, Object> properties = new HashMap<>();
        properties.put(PublishConstants.ROTATE_EVERY_KB, 1);
        properties.put(PublishConstants.NUMBER_OF_FILES, 1);

        String property = "java.io.tmpdir";
        String tempDir = System.getProperty(property);
        System.out.println("OS current temporary directory is " + tempDir);

        //properties.put(PublishConstants.FILE_NAME, tempDir+"eagle-alert.log");

        Publishment publishment = new Publishment();
        publishment.setName("testFilePublishment");
        publishment.setType(org.apache.eagle.alert.engine.publisher.impl.AlertKafkaPublisher.class.getName());
        publishment.setPolicyIds(Arrays.asList(TEST_POLICY_ID));
        publishment.setDedupIntervalMin("PT0M");
        publishment.setSerializer(org.apache.eagle.alert.engine.publisher.impl.JsonEventSerializer.class.getName());
        publishment.setProperties(properties);

        AlertStreamEvent event = AlertPublisherTestHelper.mockEvent(TEST_POLICY_ID);

        AlertFilePublisher publisher = new AlertFilePublisher();
        publisher.init(null, publishment, null);

        publisher.onAlert(event);
        publisher.close();

    }
}
