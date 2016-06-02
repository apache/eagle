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

import org.junit.Assert;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

/**
 * Created by yonzhang on 11/25/15.
 */
public class TestSiddhiExpiredEvents {
    @Test
    public void testExpiredEventsInLengthWindow() throws Exception{
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream TempStream (user string, cmd string);";
        String query = "@info(name = 'query1') from TempStream#window.length(3) "
                + " select *"
                + " insert all events into DelayedTempStream";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("TempStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"user", "open1"});
        inputHandler.send(new Object[]{"user", "open2"});
        inputHandler.send(new Object[]{"user", "open3"});
        inputHandler.send(new Object[]{"user", "open4"});
        inputHandler.send(new Object[]{"user", "open5"});
        inputHandler.send(new Object[]{"user", "open6"});
        Thread.sleep(1000);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testExpiredEventsInLengthBatchWindow() throws Exception{
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream TempStream (user string, cmd string);";
        String query = "@info(name = 'query1') from TempStream#window.lengthBatch(2) "
                + " select *"
                + " insert all events into DelayedTempStream";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("TempStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"user", "open1"});
        inputHandler.send(new Object[]{"user", "open2"});
        inputHandler.send(new Object[]{"user", "open3"});
        inputHandler.send(new Object[]{"user", "open4"});
        inputHandler.send(new Object[]{"user", "open5"});
        inputHandler.send(new Object[]{"user", "open6"});
        Thread.sleep(1000);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testExpireEvents2() throws Exception{
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream TempStream (user string, cmd string);";
        String query = "@info(name = 'query1') from TempStream#window.length(4) "
                + " select user, cmd, count(user) as cnt " +
                " group by user " +
                "having cnt > 2 "
                + " insert all events into DelayedTempStream";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("TempStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"user", "open1"});
        inputHandler.send(new Object[]{"user", "open2"});
        inputHandler.send(new Object[]{"user", "open3"});
        inputHandler.send(new Object[]{"user", "open4"});
        inputHandler.send(new Object[]{"user", "open5"});
//        inputHandler.send(new Object[]{"user", "open6"});
//        inputHandler.send(new Object[]{"user", "open7"});
//        inputHandler.send(new Object[]{"user", "open8"});
//        inputHandler.send(new Object[]{"user", "open9"});
        Thread.sleep(1000);
        executionPlanRuntime.shutdown();
    }
}
