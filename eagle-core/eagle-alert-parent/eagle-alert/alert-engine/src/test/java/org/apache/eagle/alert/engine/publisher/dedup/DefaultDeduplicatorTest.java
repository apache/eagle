/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.publisher.dedup;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.impl.DefaultDeduplicator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.eagle.alert.engine.publisher.AlertPublisherTestHelper.*;

public class DefaultDeduplicatorTest {

    @Test
    public void testNormal() throws Exception {
        //String intervalMin, List<String> customDedupFields, String dedupStateField, String dedupStateCloseValue
        // assume state: OPEN, WARN, CLOSE
        System.setProperty("config.resource", "/application-mongo-statestore.conf");
        Config config = ConfigFactory.load();
        DedupCache dedupCache = new DedupCache(config, "testPublishment");
        DefaultDeduplicator deduplicator = new DefaultDeduplicator(
            "PT1M", Arrays.asList(new String[] {"alertKey"}), "state", "close", dedupCache);

        StreamDefinition stream = createStream();
        PolicyDefinition policy = createPolicy(stream.getStreamId(), "testPolicy");

        AlertStreamEvent e1 = createEvent(stream, policy, new Object[] {
            System.currentTimeMillis(), "host1", "testPolicy-host1-01", "OPEN", 0, 0
        });
        AlertStreamEvent e2 = createEvent(stream, policy, new Object[] {
            System.currentTimeMillis(), "host1", "testPolicy-host1-01", "WARN", 0, 0
        });
        AlertStreamEvent e3 = createEvent(stream, policy, new Object[] {
            System.currentTimeMillis(), "host1", "testPolicy-host1-01", "OPEN", 0, 0
        });
        AlertStreamEvent e4 = createEvent(stream, policy, new Object[] {
            System.currentTimeMillis(), "host1", "testPolicy-host1-01", "WARN", 0, 0
        });
        AlertStreamEvent e5 = createEvent(stream, policy, new Object[] {
            System.currentTimeMillis(), "host1", "testPolicy-host1-01", "CLOSE", 0, 0
        });
        AlertStreamEvent e6 = createEvent(stream, policy, new Object[] {
            System.currentTimeMillis(), "host1", "testPolicy-host1-01", "OPEN", 0, 0
        });
        AlertStreamEvent e7 = createEvent(stream, policy, new Object[] {
            System.currentTimeMillis(), "host1", "testPolicy-host1-01", "OPEN", 0, 0
        });
        AlertStreamEvent e8 = createEvent(stream, policy, new Object[] {
            System.currentTimeMillis(), "host1", "testPolicy-host1-01", "OPEN", 0, 0
        });

        List<AlertStreamEvent> allResults = new ArrayList<AlertStreamEvent>();
        new Thread(new Runnable() {
            @Override
            public void run() {
                List<AlertStreamEvent> result = deduplicator.dedup(e1);
                if (result != null) {
                    allResults.addAll(result);
                }
                System.out.println("1 >>>> " + ToStringBuilder.reflectionToString(result));
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                List<AlertStreamEvent> result = deduplicator.dedup(e2);
                if (result != null) {
                    allResults.addAll(result);
                }
                System.out.println("2 >>>> " + ToStringBuilder.reflectionToString(result));
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                List<AlertStreamEvent> result = deduplicator.dedup(e3);
                if (result != null) {
                    allResults.addAll(result);
                }
                System.out.println("3 >>>> " + ToStringBuilder.reflectionToString(result));
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                List<AlertStreamEvent> result = deduplicator.dedup(e4);
                if (result != null) {
                    allResults.addAll(result);
                }
                System.out.println("4 >>>> " + ToStringBuilder.reflectionToString(result));
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }

                List<AlertStreamEvent> result = deduplicator.dedup(e5);
                if (result != null) {
                    allResults.addAll(result);
                }
                System.out.println("5 >>>> " + ToStringBuilder.reflectionToString(result));
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                List<AlertStreamEvent> result = deduplicator.dedup(e6);
                if (result != null) {
                    allResults.addAll(result);
                }
                System.out.println("6 >>>> " + ToStringBuilder.reflectionToString(result));
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                List<AlertStreamEvent> result = deduplicator.dedup(e7);
                if (result != null) {
                    allResults.addAll(result);
                }
                System.out.println("7 >>>> " + ToStringBuilder.reflectionToString(result));
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                List<AlertStreamEvent> result = deduplicator.dedup(e8);
                if (result != null) {
                    allResults.addAll(result);
                }
                System.out.println("8 >>>> " + ToStringBuilder.reflectionToString(result));
            }
        }).start();

        Thread.sleep(2000);

        long maxCount = 0;
        for (AlertStreamEvent event : allResults) {
            Assert.assertNotNull(event.getData()[4]);
            Assert.assertNotNull(event.getData()[5]);

            if (((Long) event.getData()[4]) > maxCount) {
                maxCount = (Long) event.getData()[4];
                System.out.println(String.format(">>>>>%s: %s", event, maxCount));
            }
        }

    }
}
