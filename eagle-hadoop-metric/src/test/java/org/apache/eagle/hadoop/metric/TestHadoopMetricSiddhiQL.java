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
package org.apache.eagle.hadoop.metric;

import junit.framework.Assert;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created on 1/17/16.
 */
public class TestHadoopMetricSiddhiQL {

    @Test
    public void testNameNodeLag() throws Exception {
        String ql = "define stream s (host string, timestamp long, metric string, component string, site string, value long);" +
                " from s[metric=='hadoop.namenode.dfs.lastwrittentransactionid' and host=='a' ]#window.externalTime(timestamp, 5 min) select * insert into tmp1;" +
                " from s[metric=='hadoop.namenode.dfs.lastwrittentransactionid' and host=='b' ]#window.externalTime(timestamp, 5 min) select * insert into tmp2;" +
                " from tmp1 , tmp2 select tmp1.timestamp as t1time, max(tmp1.value) - max(tmp2.value) as gap insert into tmp3;" +
                " from tmp3[gap > 100] insert into tmp;"
                ;

        SiddhiManager sm = new SiddhiManager();
        ExecutionPlanRuntime runtime = sm.createExecutionPlanRuntime(ql);

        InputHandler input = runtime.getInputHandler("s");

        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        runtime.addCallback("tmp", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                count.incrementAndGet();
                latch.countDown();
            }
        });

        runtime.start();

        List<Event> events = generateEvents();

        for (Event e : events) {
            input.send(e);
        }

        latch.await(10, TimeUnit.SECONDS);

        Assert.assertTrue(count.get() == 1);

        runtime.shutdown();
    }

    private List<Event> generateEvents() {
        List<Event> events = new LinkedList<>();

        long base1 = System.currentTimeMillis();
        long tbase1 = 1000;
        long tbase2 = 1000;
        // {"host": "eagle-c3-lvs01-1-9953.lvs01.dev.ebayc3.com", "timestamp": 1453039016336, "metric": "hadoop.namenode.dfs.lastwrittentransactionid", "component": "namenode", "site": "sandbox", "value": "47946"}

        int SIZE = 10;
        // master / slave in sync
        for (int i = 0;i < SIZE; i++) {
            base1 += 1000;
            tbase1 += 100;
            Event e = new Event();
            e.setData(new Object[] {"a", base1, "hadoop.namenode.dfs.lastwrittentransactionid", "namenode", "sandbox", tbase1});
            events.add(e);

            tbase2 += 100;
            e = new Event();
            e.setData(new Object[] {"b", base1, "hadoop.namenode.dfs.lastwrittentransactionid", "namenode", "sandbox", tbase2});
            events.add(e);
        }


        {
            // make sure flush previous windows

            base1 += 6 * 60 * 1000;
            tbase1 = 3000;
            Event e = new Event();
            e.setData(new Object[]{"a", base1, "hadoop.namenode.dfs.lastwrittentransactionid", "namenode", "sandbox", tbase1});
            events.add(e);

            tbase2 = tbase1 + 110; // > 100, trigger an event
            e = new Event();
            e.setData(new Object[]{"b", base1, "hadoop.namenode.dfs.lastwrittentransactionid", "namenode", "sandbox", tbase2});
            events.add(e);

            // trigger event
            base1 = base1 + 100;
            e = new Event();
            e.setData(new Object[]{"b", base1, "hadoop.namenode.dfs.lastwrittentransactionid", "namenode", "sandbox", tbase2});
            events.add(e);
        }

        return events;
    }
}
