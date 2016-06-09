/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.state;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import org.wso2.siddhi.core.util.EventPrinter;

public class TestAggregation {
    @Test
    public void test01DownSampling() throws Exception {
        String stream = "define stream jmxMetric(cpu double, memory int, bytesIn int, bytesOut long, timestamp long);";
        String query = "@info(name = 'downSample') "
                + "from jmxMetric#window.timeBatch(1 sec) "
                + "select "
                + " min(cpu) as minCpu, max(cpu) as maxCpu, avg(cpu) as avgCpu, "
                + " min(memory) as minMem, max(memory) as maxMem, avg(memory) as avgMem, "
                + " min(bytesIn) as minBytesIn, max(bytesIn) as maxBytesIn, avg(bytesIn) as avgBytesIn, sum(bytesIn) as totalBytesIn, "
                + " min(bytesOut) as minBytesOut, max(bytesOut) as maxBytesOut, avg(bytesOut) as avgBytesOut, sum(bytesOut) as totalBytesOut, "
                + " timestamp as timeWindowEnds "
                + " INSERT  INTO tmp;";

        SiddhiManager sm = new SiddhiManager();
        ExecutionPlanRuntime plan = sm.createExecutionPlanRuntime(stream + query);

        final AtomicInteger counter = new AtomicInteger();
        plan.addCallback("downSample", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                int count = counter.incrementAndGet();
                if (count == 1) {
                    Assert.assertEquals(6000L, inEvents[0].getData(9));
                } else if(count == 2) {
                    Assert.assertEquals(6000L, inEvents[0].getData(9));
                }
            }
        });
        InputHandler input = plan.getInputHandler("jmxMetric");

        plan.start();
        sendEvent(input);
        Thread.sleep(100);
        sendEvent(input);
        Thread.sleep(1000);
        sendEvent(input);
        Thread.sleep(1000);
        sendEvent(input);
        Thread.sleep(200);
        plan.shutdown();
    }

    // send 3 events
    private void sendEvent(InputHandler input) throws Exception {
        int len = 3;
        Event[] events = new Event[len];
        for (int i = 0; i < len; i++) {
            long externalTs = System.currentTimeMillis();
            // cpu int, memory int, bytesIn long, bytesOut long, timestamp long
            events[i] = new Event(externalTs + i, new Object[] {
                    15.0,
                    15,
                    1000,
                    2000L,
                    externalTs + i
            });
        }

        for (Event e : events) {
            input.send(e);
        }
    }
}
