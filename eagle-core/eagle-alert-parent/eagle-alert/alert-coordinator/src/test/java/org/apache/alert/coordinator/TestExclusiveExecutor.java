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

import com.google.common.base.Joiner;
import org.apache.eagle.alert.config.ZKConfig;
import org.apache.eagle.alert.coordinator.ExclusiveExecutor;
import org.apache.eagle.alert.utils.ZookeeperEmbedded;
import org.junit.*;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@Ignore
public class TestExclusiveExecutor {

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
    public void testConcurrency() throws Exception {
        ByteArrayOutputStream newStreamOutput = new ByteArrayOutputStream();
        PrintStream newStream = new PrintStream(newStreamOutput);
        PrintStream oldStream = System.out;

        System.setOut(newStream);

        ZKConfig zkConfig = new ZKConfig();
        zkConfig.zkQuorum = "127.0.0.1:2181";
        zkConfig.zkRetryTimes = 3;
        zkConfig.zkRoot = "/";
        zkConfig.connectionTimeoutMs  = 3000;
        zkConfig.zkRetryInterval  = 1000;
        zkConfig.zkSessionTimeoutMs = 5000;

        String path = "/concurrenty";
        AtomicBoolean lock1 = new AtomicBoolean(false);
        Runnable runnableOne = () -> { System.out.println("this is thread one"); lock1.set(true);};
        new Thread(() -> {
            ExclusiveExecutor executor = new ExclusiveExecutor(zkConfig);
            try {
                executor.execute(path, runnableOne);
            } catch (TimeoutException e) {
            }
        }).start();


        AtomicBoolean lock2 = new AtomicBoolean();
        Runnable runnableTwo = () ->  { System.out.println("this is thread two"); lock2.set(true);};
        new Thread(() -> {
            ExclusiveExecutor executor = new ExclusiveExecutor(zkConfig);
            try {
                executor.execute(path, runnableTwo);
            } catch (TimeoutException e) {
            }
        }).start();

        Thread.sleep(2000);

        System.out.flush();
        BufferedReader br = new BufferedReader(new StringReader(newStreamOutput.toString()));
        List<String> logs = new ArrayList<String>();
        String line = null;
        while ((line = br.readLine()) != null) {
            logs.add(line);
        }

        System.setOut(oldStream);
        System.out.println("Cached logs: " + Joiner.on("\n").join(logs));

        Assert.assertTrue(logs.stream().anyMatch((log) -> log.contains("this is thread one")));
        Assert.assertTrue(logs.stream().anyMatch((log) -> log.contains("this is thread two")));

        Assert.assertTrue(lock1.get());
        Assert.assertTrue(lock2.get());
    }

}
