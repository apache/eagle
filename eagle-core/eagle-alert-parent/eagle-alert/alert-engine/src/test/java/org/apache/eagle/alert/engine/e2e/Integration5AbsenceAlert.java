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
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Since 6/29/16.
 */
public class Integration5AbsenceAlert {
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
    @Test @Ignore
    public void testTriggerAbsenceAlert() throws Exception{
        System.setProperty("config.resource", "/absence/application-absence.conf");
        ConfigFactory.invalidateCaches();
        Config config = ConfigFactory.load();

        System.out.println("loading metadatas...");
        Integration1.loadMetadatas("/absence/", config);
        System.out.println("loading metadatas done!");


        executors.submit(() -> {
            try {
                UnitTopologyMain.main(args);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // wait 20 seconds for topology to bring up
        try{
            Thread.sleep(20000);
        }catch(Exception ex){}

        // send mock data
        executors.submit(() -> {
            try {
                SampleClient5AbsenceAlert.main(args);
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
}