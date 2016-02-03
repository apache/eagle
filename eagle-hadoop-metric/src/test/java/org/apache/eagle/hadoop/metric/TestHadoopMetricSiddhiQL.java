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
import org.junit.Ignore;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
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

    @Ignore
    @Test
    public void testNameNodeLag() throws Exception {
        String ql = "define stream s (host string, timestamp long, metric string, component string, site string, value string);" +
                " @info(name='query') " +
                " from s[metric=='hadoop.namenode.dfs.lastwrittentransactionid' and host=='a' ]#window.externalTime(timestamp, 5 min) select * insert into tmp1;" +
                " from s[metric=='hadoop.namenode.dfs.lastwrittentransactionid' and host=='b' ]#window.externalTime(timestamp, 5 min) select * insert into tmp2;" +
                " from tmp1 , tmp2 select tmp1.timestamp as t1time, tmp2.timestamp as t2time, max(convert(tmp1.value, 'long')) - max(convert(tmp2.value, 'long')) as gap insert into tmp3;" +
                " from tmp3[gap > 100] select * insert into tmp;"
                ;

        System.out.println("test name node log with multiple stream defined!");
        testQL(ql, generateNameNodeLagEvents(), 2, true);
    }

    @Ignore
    @Test
    public void testNameNodeLag2_patternMatching() throws Exception {
        String ql =
            " define stream s (host string, timestamp long, metric string, component string, site string, value string); " +
            " @info(name='query') " +
            " from every a = s[metric=='hadoop.namenode.dfs.lastwrittentransactionid'] " +
            "         -> b = s[metric=='hadoop.namenode.dfs.lastwrittentransactionid' and b.host != a.host " +
                    " and (convert(a.value, 'long') + 100) < convert(value, 'long') ] " +
            " within 5 min select a.host as hostA, b.host as hostB insert into tmp; ";

        testQL(ql, generateNameNodeLagEvents(), 21);
    }

    private void testQL(String ql, List<Event> events, int i) throws Exception {
        testQL(ql, events, i, false);
    }

    private void testQL(String ql, List<Event> events, int eventHappenCount, boolean useStreamCallback) throws InterruptedException {
        SiddhiManager sm = new SiddhiManager();
        ExecutionPlanRuntime runtime = sm.createExecutionPlanRuntime(ql);

        InputHandler input = runtime.getInputHandler("s");

        final AtomicInteger count = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        // use stream call back or query callback
        if (useStreamCallback) {
            runtime.addCallback("tmp", new StreamCallback() {
                AtomicInteger round = new AtomicInteger();

                @Override
                public void receive(Event[] events) {
                    count.incrementAndGet();
                    round.incrementAndGet();
                    System.out.println("event round: " + round.get() + " event count : " + events.length);
                    printEvents(events);
                    latch.countDown();
                }
            });
        } else {
            runtime.addCallback("query", new QueryCallback() {
                AtomicInteger round = new AtomicInteger();

                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    count.incrementAndGet();
                    round.incrementAndGet();
                    System.out.println("event round: " + round.get() + " event count : " + inEvents.length);
                    printEvents(inEvents);
                    latch.countDown();
                }
            });
        }

        runtime.start();

        for (Event e : events) {
            input.send(e);
        }

        latch.await(10, TimeUnit.SECONDS);
        Thread.sleep(10000);

        System.out.println(count.get());
        Assert.assertEquals(eventHappenCount, count.get());

        runtime.shutdown();
        sm.shutdown();
    }

    private void printEvents(Event[] inEvents) {
        for (Event e : inEvents) {
            for(Object o : e.getData()) {
                System.out.print(o);
                System.out.print('\t');
            }
            System.out.println();
        }
    }

    private List<Event> generateNameNodeLagEvents() {
        List<Event> events = new LinkedList<>();

        long base1 = System.currentTimeMillis();
        long tbase1 = 1000;
        long tbase2 = 1000;

        int SIZE = 10;
        // master / slave in sync, no events for these
        for (int i = 0;i < SIZE; i++) {
            base1 += 1000;
            tbase1 += 100;
            Event e = new Event();
            e.setData(new Object[] {"a", base1, "hadoop.namenode.dfs.lastwrittentransactionid", "namenode", "sandbox", String.valueOf(tbase1)});
            events.add(e);

            tbase2 += 100;
            e = new Event();
            e.setData(new Object[] {"b", base1, "hadoop.namenode.dfs.lastwrittentransactionid", "namenode", "sandbox", String.valueOf(tbase2)});
            events.add(e);
        }


        {
            // make sure flush previous windows

            base1 += 6 * 60 * 1000;
            tbase1 = 3000;
            Event e = new Event();
            e.setData(new Object[]{"a", base1, "hadoop.namenode.dfs.lastwrittentransactionid", "namenode", "sandbox", String.valueOf(tbase1)});
            events.add(e);

            tbase2 = tbase1 + 110; // > 100, trigger an event
            e = new Event();
            e.setData(new Object[]{"b", base1, "hadoop.namenode.dfs.lastwrittentransactionid", "namenode", "sandbox", String.valueOf(tbase2)});
            events.add(e);

            // trigger event
//            base1 = base1 + 6 * 60 * 1000;
//            e = new Event();
//            e.setData(new Object[]{"b", base1, "hadoop.namenode.dfs.lastwrittentransactionid", "namenode", "sandbox", String.valueOf(tbase2)});
//            events.add(e);
        }

        return events;
    }

    /**
    E.g. Alert if temperature of a room increases by 5 degrees within 10 min.
            from every( e1=TempStream ) -> e2=TempStream[e1.roomNo==roomNo and (e1.temp + 5) <= temp ]
                within 10 min
            select e1.roomNo, e1.temp as initialTemp, e2.temp as finalTemp
            insert into AlertStream;
     */
    @Ignore
    @Test
    public void testCase4_LiveDataNodeJoggle() throws Exception {

        String ql = "define stream s (host string, timestamp long, metric string, component string, site string, value string);" +
                " @info(name='query') " +
                " from every (e1 = s[metric == 'hadoop.namenode.fsnamesystemstate.numlivedatanodes' ]) -> " +
                "             e2 = s[metric == e1.metric and host == e1.host and (convert(e1.value, 'long') + 5) <= convert(value, 'long') ]" +
                " within 5 min " +
                " select e1.metric, e1.host, e1.value as lowNum, e1.timestamp as start, e2.value as highNum, e2.timestamp as end " +
                " insert into tmp;"
                ;

        testQL(ql, generateDataNodeJoggleEvents(), 10);
    }

    private List<Event> generateDataNodeJoggleEvents() {
        List<Event> events = new LinkedList<>();

        long base1 = System.currentTimeMillis();
        long tbase1 = 1000;
        long tbase2 = 5000;

        int SIZE = 10;
        // master / slave in sync
        for (int i = 0;i < SIZE; i++) {
            base1 += 1000;

            Event e = new Event();
            e.setData(new Object[] {"a", base1, "hadoop.namenode.fsnamesystemstate.numlivedatanodes", "namenode", "sandbox", String.valueOf(tbase1)});
            events.add(e);

            // inject b events, to test host a not disturb by this metric stream
            e = new Event();
            e.setData(new Object[] {"b", base1, "hadoop.namenode.fsnamesystemstate.numlivedatanodes", "namenode", "sandbox", String.valueOf(tbase2)});
            events.add(e);
        }

        {
            // insert an invalid
            base1 += 1 * 60 * 1000;
            tbase1 = 3000;
            Event e = new Event();
            e.setData(new Object[]{"a", base1, "hadoop.namenode.fsnamesystemstate.numlivedatanodes", "namenode", "sandbox", String.valueOf(tbase1)});
            events.add(e);

            // trigger event, we dont really care about this event value, just make sure above metri triggered
            base1 = base1 + 100;
            e = new Event();
            e.setData(new Object[]{"b", base1, "hadoop.namenode.dfs.lastwrittentransactionid", "namenode", "sandbox", String.valueOf(tbase2)});
            events.add(e);
        }

        return events;
    }
}
