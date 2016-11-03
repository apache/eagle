/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  * <p/>
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  * <p/>
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.eagle.common.agg;

import org.junit.Assert;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Since 8/3/16.
 */
public class TestSiddhiExternalTimeBatch {
    @Test
    public void testSiddhi() throws Exception {
        String ql = "define stream s (host string, timestamp long, metric string, site string, value double);" +
            " @info(name='query') " +
            " from s[metric == \"missingblocks\"]#window.externalTimeBatch(timestamp, 1 min, 0) select host, count(value) as avg group by host insert into tmp; ";
        System.out.println("query: " + ql);
        SiddhiManager sm = new SiddhiManager();
        ExecutionPlanRuntime runtime = sm.createExecutionPlanRuntime(ql);

        InputHandler input = runtime.getInputHandler("s");

        AtomicInteger index = new AtomicInteger(0);

        runtime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                printEvents(inEvents);
                if (index.get() == 0) {
                    Assert.assertEquals(3, inEvents.length);
                    Assert.assertEquals("host1", inEvents[0].getData()[0]);
                    Assert.assertEquals(3L, inEvents[0].getData()[1]);
                    Assert.assertEquals("host2", inEvents[1].getData()[0]);
                    Assert.assertEquals(4L, inEvents[1].getData()[1]);
                    Assert.assertEquals("host3", inEvents[2].getData()[0]);
                    Assert.assertEquals(2L, inEvents[2].getData()[1]);
                    index.incrementAndGet();
                } else if (index.get() == 1) {
                    Assert.assertEquals(3, inEvents.length);
                    Assert.assertEquals("host1", inEvents[0].getData()[0]);
                    Assert.assertEquals(1L, inEvents[0].getData()[1]);
                    Assert.assertEquals("host2", inEvents[1].getData()[0]);
                    Assert.assertEquals(2L, inEvents[1].getData()[1]);
                    Assert.assertEquals("host3", inEvents[2].getData()[0]);
                    Assert.assertEquals(2L, inEvents[2].getData()[1]);
                    index.incrementAndGet();
                }
            }
        });
        runtime.start();

        sendEvents(3, 4, 2, input, 1000L);
        Thread.sleep(1000);
        sendEvents(1, 2, 2, input, 61000L);
        sendEvents(3, 10, 7, input, 121000L);
        runtime.shutdown();
        sm.shutdown();
        Thread.sleep(1000);
    }

    void sendEvents(int countHost1, int countHost2, int countHost3, InputHandler input, long startTime) throws Exception {
        for (int i = 0; i < countHost1; i++) {
            Event e = createEvent("host1", startTime + i * 100);
            input.send(e);
        }
        startTime += 2000;
        for (int i = 0; i < countHost2; i++) {
            Event e = createEvent("host2", startTime + i * 100);
            input.send(e);
        }
        startTime += 4000;
        for (int i = 0; i < countHost3; i++) {
            Event e = createEvent("host3", startTime + i * 100);
            input.send(e);
        }
    }

    void printEvents(Event[] inEvents) {
        for (Event e : inEvents) {
            System.out.print(e);
            System.out.print(",");
        }
        System.out.println();
    }

    Event createEvent(String host, long timestamp) {
        Event e = new Event();
        e.setTimestamp(timestamp);
        e.setData(new Object[] {host, timestamp, "missingblocks", "site1", 14.0});
        return e;
    }

}
