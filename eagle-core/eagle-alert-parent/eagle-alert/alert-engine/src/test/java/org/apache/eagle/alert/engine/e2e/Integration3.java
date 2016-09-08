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
package org.apache.eagle.alert.engine.e2e;

import backtype.storm.utils.Utils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.engine.UnitTopologyMain;
import org.apache.eagle.alert.utils.KafkaEmbedded;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @since May 10, 2016
 */
public class Integration3 {

    private String[] args;
    private ExecutorService executors = Executors.newFixedThreadPool(5);
    private static KafkaEmbedded kafka;

    @BeforeClass
    public static void setup() {
        // FIXME : start local kafka
    }

    @AfterClass
    public static void end() {
        if (kafka != null) {
            kafka.shutdown();
        }
    }

    /**
     * Assumption:
     * <p>
     * start metadata service 8080 /rest
     * <p>
     * <pre>
     * user@kafka-host:~$ $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic syslog_events
     * </pre>
     * <p>
     *
     * @throws InterruptedException
     */
    @Test
    public void testSeverity() throws Exception {
        System.setProperty("config.resource", "/e2e/application-e2e.conf");
        ConfigFactory.invalidateCaches();
        Config config = ConfigFactory.load();

        System.out.println("loading metadatas...");
        Integration1.loadMetadatas("/e2e/", config);
        System.out.println("loading metadatas done!");

        // send sample sherlock data
        executors.submit(() -> {
            try {
                SampleClient3.main(args);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        executors.submit(() -> {
            try {
                UnitTopologyMain.main(args);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Utils.sleep(1000 * 5l);
        while (true) {
            Integration1.proactive_schedule(config);

            Utils.sleep(1000 * 60l * 5);
        }
    }

    @Test
    public void testJson() throws Exception {
        Integration1.checkAll("/e2e/");
    }

}
