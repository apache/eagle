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
package org.apache.eagle.alert.cep;

import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import junit.framework.Assert;

/**
 * @since Dec 21, 2015
 *
 */
public class TestAggregation2 {
    private final static String EXECUTION_PLAN_NAME = "query";

    public static class C3EsEntity {
        public String logLevel;
        public String srcHost;
        public String type;
        public String subtype;
        public long timestamp;
        public String message;
        public String tag;
    }
    
    public static class C3ConfigEntity {
        public long timestamp;
        public String owner;
        public String description;
    }

    /**
     * Start siddhi runtime, populate with some test log, try to get a changed
     * stream
     * 
     * 10 Min
     * 
     * 
     * @throws InterruptedException
     */
    @Test
    public void siddhiTest() throws InterruptedException {
        SiddhiManager sm = new SiddhiManager();
        StringBuilder siddhiQL = new StringBuilder(
                " define stream c3EsStream (logLevel string, srcHost string, type string, subtype string, timestamp long, message string, tag string); ");
//        siddhiQL.append(" define stream deployStream(timestamp long, owner string, description string);");
        siddhiQL.append(" @info(name = '" + EXECUTION_PLAN_NAME + "') ");
//        siddhiQL.append(" from c3EsStream[logLevel=='ERROR']  #window.time(10 sec) ,");
        siddhiQL.append(" from c3EsStream#window.timeBatch(3 sec) ");
        siddhiQL.append(" select logLevel, srcHost, type, subtype, tag, timestamp, message ");//, count(1) as errorCount ");// group by tag ");
        siddhiQL.append(" insert into tempStream; ");
//        siddhiQL.append(" from deployStream #window.time(24 hour) ");
//        siddhiQL.append(" select * insert into deployStream ");
//        siddhiQL.append(" from t = tempStream, d = deployStream ");
//        siddhiQL.append(" select * insert into outputStream;");
        
        ExecutionPlanRuntime runtime = sm.createExecutionPlanRuntime(siddhiQL.toString());
        InputHandler c3LogHandler = runtime.getInputHandler("c3EsStream");
//        InputHandler deployActionHandler = runtime.getInputHandler("deployStream");

        final AtomicInteger count = new AtomicInteger();
        runtime.addCallback(EXECUTION_PLAN_NAME, new QueryCallback() {
            @Override
            public void receive(long arg0, Event[] inevents, Event[] removeevents) {
                int currentCount = count.addAndGet(inevents.length);
                System.out.println(MessageFormat.format("{0} : Round {1} ====", System.currentTimeMillis(), currentCount));
                System.out.println(" events count " + inevents.length);

                for (Event e : inevents) {
                    Object[] tranformedData = e.getData();
                    for (Object o : tranformedData) {
                        System.out.print(o);
                        System.out.print(' ');
                    }
                    System.out.println(" events endendend");
                }
            }

        });
        runtime.start();

//        List<C3ConfigEntity> configStream = fakeConfigStream();
//        for (C3ConfigEntity config : configStream) {
//            Event event = new Event();
//            event.setData(new Object[] { config.timestamp, config.owner, config.description });
//            deployActionHandler.send(event);
//        }

        List<C3EsEntity> streams = fakeStream();

        for (C3EsEntity entity : streams) {
            Event e = new Event();
            e.setData(new Object[] { entity.logLevel, entity.srcHost, entity.type, entity.subtype, entity.timestamp,
                    entity.message, entity.tag });
            c3LogHandler.send(e);
            Thread.sleep(600);
        }
        
        Thread.sleep(10000);
        runtime.shutdown();
        System.out.println("all events received in call back : " + count);
        Assert.assertEquals(2, count.get());
    }

    private List<C3ConfigEntity> fakeConfigStream() {
        long base = System.currentTimeMillis();
        List<C3ConfigEntity> configs = new LinkedList<C3ConfigEntity>();
        for (int i = 0; i < 10; i++) {
            C3ConfigEntity config = new C3ConfigEntity();
            config.description = "upgrade";
            config.owner = "huai";
            config.timestamp = base + 1000;
        }
        return configs;
    }

    private List<C3EsEntity> fakeStream() {
        long base = System.currentTimeMillis();
        List<C3EsEntity> data = new LinkedList<C3EsEntity>();
        for (int i = 0; i < 10; i++) {
            C3EsEntity e = new C3EsEntity();
            if (i % 2 == 0) {
                e.logLevel = "ERROR";
            } else {
                e.logLevel = "TRACE";
            }
            e.timestamp = base;
            base += 2000;
            e.srcHost = "localhost";
            e.type = "nova";
            e.subtype = "nova.osapi_compute.wsgi.server";
            e.message = "error-logs-" + i;
            if (i % 2 == 0) {
                e.tag = "nova-type";
            } else {
                e.tag = "nova-type-2";
            }

            data.add(e);
        }
        return data;
    }
}
