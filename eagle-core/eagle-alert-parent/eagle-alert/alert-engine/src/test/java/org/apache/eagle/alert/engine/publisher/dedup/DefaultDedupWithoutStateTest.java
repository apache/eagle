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

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.impl.DefaultDeduplicator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.apache.eagle.alert.engine.publisher.AlertPublisherTestHelper.*;

public class DefaultDedupWithoutStateTest {

    @Test
    public void testNormal() throws Exception {
        //String intervalMin, List<String> customDedupFields, String dedupStateField, String dedupStateCloseValue
        // assume state: OPEN, WARN, CLOSE
        System.setProperty("config.resource", "/application-mongo-statestore.conf");
        Config config = ConfigFactory.load();
        DedupCache dedupCache = new DedupCache(config, "testPublishment");
        DefaultDeduplicator deduplicator = new DefaultDeduplicator(
            "PT10S", Arrays.asList(new String[] {"alertKey"}), null, null, dedupCache);

        StreamDefinition stream = createStream();
        PolicyDefinition policy = createPolicy(stream.getStreamId(), "testPolicy");

        int[] hostIndex = new int[] {1, 2, 3};
        String[] states = new String[] {"OPEN", "WARN", "CLOSE"};
        Random random = new Random();

        final ConcurrentLinkedDeque<AlertStreamEvent> nonDedupResult = new ConcurrentLinkedDeque<AlertStreamEvent>();

        for (int i = 0; i < 100; i++) {
            new Thread(new Runnable() {

                @Override
                public void run() {
                    int index = hostIndex[random.nextInt(3)];
                    AlertStreamEvent e1 = createEvent(stream, policy, new Object[] {
                        System.currentTimeMillis(), "host" + index,
                        String.format("testPolicy-host%s-01", index),
                        states[random.nextInt(3)], 0, 0
                    });
                    List<AlertStreamEvent> result = deduplicator.dedup(e1);
                    if (result != null) {
                        System.out.println(">>>" + Joiner.on(",").join(result));
                        nonDedupResult.addAll(result);
                    } else {
                        System.out.println(">>>" + result);
                    }
                }

            }).start();
        }

        Thread.sleep(1000);

        System.out.println("old size: " + nonDedupResult.size());
        Assert.assertTrue(nonDedupResult.size() > 0 && nonDedupResult.size() <= 3);

        Thread.sleep(15000);

        for (int i = 0; i < 100; i++) {
            new Thread(new Runnable() {

                @Override
                public void run() {
                    int index = hostIndex[random.nextInt(3)];
                    AlertStreamEvent e1 = createEvent(stream, policy, new Object[] {
                        System.currentTimeMillis(), "host" + index,
                        String.format("testPolicy-host%s-01", index),
                        states[random.nextInt(3)], 0, 0
                    });
                    List<AlertStreamEvent> result = deduplicator.dedup(e1);
                    if (result != null) {
                        System.out.println(">>>" + Joiner.on(",").join(result));
                        nonDedupResult.addAll(result);
                    } else {
                        System.out.println(">>>" + result);
                    }
                }

            }).start();
        }

        Thread.sleep(1000);

        System.out.println("new size: " + nonDedupResult.size());
        Assert.assertTrue(nonDedupResult.size() > 3 && nonDedupResult.size() <= 6);
    }
}
