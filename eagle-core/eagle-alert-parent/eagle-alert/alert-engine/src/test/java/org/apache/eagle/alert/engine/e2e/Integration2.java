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
import org.junit.Ignore;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @since May 10, 2016
 */
public class Integration2 {

    private ExecutorService executors = Executors.newFixedThreadPool(5);

    /**
     * <pre>
     * Create topic
     * liasu@xxxx:~$ $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic eslogs
     * liasu@xxxx:~$ $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bootfailures
     * </pre>
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Integration2 inte = new Integration2();
        inte.args = args;
        inte.test_start();
    }

    private String[] args;

    @Ignore
    @Test
    public void test_start() throws Exception {
        System.setProperty("config.resource", "/correlation/application-integration-2.conf");
        ConfigFactory.invalidateCaches();
        Config config = ConfigFactory.load();
        Integration1.loadMetadatas("/correlation/", config);

        executors.submit(() -> {
            try {
                UnitTopologyMain.main(args);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        executors.submit(() -> SampleClient2.main(args));

        Utils.sleep(1000 * 5l);
        while (true) {
            Integration1.proactive_schedule(config);
            Utils.sleep(1000 * 60l * 5);
        }
    }

    @Test
    @Ignore
    public void test3() throws Exception {
        SiddhiManager sm = new SiddhiManager();
        String s1 = " define stream esStream(instanceUuid string, timestamp long, logLevel string, message string, reqId string, host string, component string); ";
        s1 += " define stream ifStream(instanceUuid string, timestamp long, reqId string, message string, host string); ";
        s1 += "from esStream#window.externalTime(timestamp, 20 min) as a join ifStream#window.externalTime(timestamp, 5 min) as b on a.instanceUuid == b.instanceUuid  within 10 min select logLevel, a.host as aHost, a.component, a.message as logMessage, b.message as failMessage, a.timestamp as t1, b.timestamp as t2, b.host as bHost, count(1) as errorCount group by component insert into log_stream_join_output; ";
        ExecutionPlanRuntime epr = sm.createExecutionPlanRuntime(s1);

        epr.addCallback("log_stream_join_output", new StreamCallback() {
            @Override
            public void receive(Event[] arg0) {
                System.out.println("join result!");
                EventPrinter.print(arg0);
            }
        });

        InputHandler input1 = epr.getInputHandler("esStream");
        InputHandler input2 = epr.getInputHandler("ifStream");

        epr.start();

        long base = 1462880695837l;

        while (true) {
            sendEvent(input1, input2, base);

            base = base + 3000;

            Utils.sleep(3000);
        }

    }

    private void sendEvent(InputHandler input1, InputHandler input2, long base) throws InterruptedException {
        {
            Event e = new Event();
            e.setTimestamp(base);
            e.setData(new Object[] {
                "instance-guid-c2a1c926-b590-418e-bf57-41469d7891fa",
                base,
                "ERROR",
                "NullPointException",
                "req-id-82dab92c-9e45-4ad8-8793-96e912831f00",
                "nova.host",
                "NOVA"
            });
            input1.send(e);
        }

        {
            Event e = new Event();
            e.setTimestamp(base);
            e.setData(new Object[] {"instance-guid-c2a1c926-b590-418e-bf57-41469d7891fa",
                base,
                "req-id-82dab92c-9e45-4ad8-8793-96e912831f00",
                "boot failure for when try start the given vm!",
                "boot-vm-data-center.corp.com"});
            input2.send(e);
        }
    }

}
