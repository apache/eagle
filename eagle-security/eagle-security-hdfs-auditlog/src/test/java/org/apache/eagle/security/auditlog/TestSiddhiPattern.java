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
package org.apache.eagle.security.auditlog;

import org.apache.eagle.common.DateTimeUtil;
import org.junit.Test;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;

public class TestSiddhiPattern {
    @Test
    public void testPattern() throws Exception{
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream eventStream (timeStamp long, user string, src string, cmd string);";
        String query = "@info(name = 'query1') from " +
                "every a = eventStream[cmd=='getfileinfo'] " +
                "-> b = eventStream[cmd=='append' and user==a.user and src==a.src] " +
                "-> c = eventStream[cmd=='getfileinfo'and user==a.user and src==a.src] " +
                "select a.user as user, b.cmd as cmd, a.src as src " +
                "insert into outputStreams";

        SiddhiAppRuntime SiddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);

        SiddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        InputHandler inputHandler = SiddhiAppRuntime.getInputHandler("eventStream");
        SiddhiAppRuntime.start();
        long curTime = DateTimeUtil.humanDateToMilliseconds("2015-09-17 00:00:00,000");
        System.out.println("curTime : " + curTime);
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private", "getfileinfo"});
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private", "append"});
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private1", "getfileinfo"});
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private1", "open"});
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private1", "append"});
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private", "getfileinfo"});
        Thread.sleep(100);
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private1", "getfileinfo"});
        Thread.sleep(100);
        SiddhiAppRuntime.shutdown();

    }

    @Test
    public void testMultiplePatterns() throws Exception{
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream eventStream (timeStamp long, user string, src string, cmd string);";
        String query1 = "@info(name = 'query1') from " +
                "every a = eventStream[cmd=='getfileinfo'] " +
                "-> b = eventStream[cmd=='append' and user==a.user and src==a.src] " +
                "-> c = eventStream[cmd=='getfileinfo'and user==a.user and src==a.src] " +
                "select a.user as user, b.cmd as cmd, a.src as src " +
                "insert into outputStream";
        String query2 = ";@info(name = 'query2') from " +
                "every a = eventStream[cmd=='getfileinfo'] " +
                "-> b = eventStream[cmd=='open' and user==a.user and src==a.src] " +
                "select a.user as user, b.cmd as cmd, a.src as src " +
                "insert into outputStream";

        SiddhiAppRuntime SiddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query1+query2);

        SiddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
        SiddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        InputHandler inputHandler = SiddhiAppRuntime.getInputHandler("eventStream");
        SiddhiAppRuntime.start();
        long curTime = DateTimeUtil.humanDateToMilliseconds("2015-09-17 00:00:00,000");
        System.out.println("curTime : " + curTime);
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private", "getfileinfo"});
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private", "append"});
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private1", "getfileinfo"});
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private1", "open"});
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private1", "append"});
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private", "getfileinfo"});
        Thread.sleep(100);
        inputHandler.send(new Object[]{curTime, "user", "/tmp/private1", "getfileinfo"});
        Thread.sleep(100);
        SiddhiAppRuntime.shutdown();

    }
}
