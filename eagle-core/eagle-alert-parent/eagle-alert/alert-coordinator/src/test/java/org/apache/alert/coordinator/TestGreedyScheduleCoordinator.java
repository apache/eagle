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
package org.apache.alert.coordinator;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.config.ZKConfig;
import org.apache.eagle.alert.config.ZKConfigBuilder;
import org.apache.eagle.alert.coordinator.ExclusiveExecutor;
import org.apache.eagle.alert.utils.ZookeeperEmbedded;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestGreedyScheduleCoordinator {

    public static class GreedyScheduleCoordinator {

        public int schedule(int input) throws TimeoutException {
            Config config = ConfigFactory.load().getConfig("coordinator");
            ZKConfig zkConfig = ZKConfigBuilder.getZKConfig(config);
            ExclusiveExecutor executor = new ExclusiveExecutor(zkConfig);
            final AtomicInteger r = new AtomicInteger();
            executor.execute("/alert/test", () -> {
                try {
                    Thread.sleep(input);
                } catch (Exception e){
                }

                r.set(input);
            });
            try {
                executor.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            throw new RuntimeException("Acquire greedy scheduler lock failed, please retry later");
        }
    }

    ZookeeperEmbedded zkEmbed;

    @Before
    public void setUp() throws Exception {
        zkEmbed = new ZookeeperEmbedded(2181);
        zkEmbed.start();

        Thread.sleep(2000);
    }

    @After
    public void tearDown() throws Exception {
        zkEmbed.shutdown();
    }

    @Test
    public void testMain() throws Exception {
        final GreedyScheduleCoordinator coordinator = new GreedyScheduleCoordinator();


        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    System.out.println("output: " + coordinator.schedule(1));
                } catch (TimeoutException e) {
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }

        }).start();

        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    System.out.println("output: " + coordinator.schedule(2));
                } catch (TimeoutException e) {
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }

        }).start();

        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    System.out.println("output: " + coordinator.schedule(3));
                } catch (TimeoutException e) {
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }

        }).start();

        Thread.sleep(15000);
    }


}
