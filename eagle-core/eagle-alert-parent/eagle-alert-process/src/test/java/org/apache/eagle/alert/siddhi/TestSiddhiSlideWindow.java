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
package org.apache.eagle.alert.siddhi;

import org.junit.Assert;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import org.apache.eagle.common.DateTimeUtil;

public class TestSiddhiSlideWindow {

    int alertCount = 0;

    @Test
    public void testSlideWindow1() throws Exception{
        alertCount = 0;
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream eventStream (user string, path string, cmd string);";
//        String query = "@info(name = 'query1') from eventStream[cmd == 'open' AND str:regexp(path, '/usr/data/[0-9]+/[0-9]+/[0-9]+')]#window.time(1 sec)"
//        		     + " select user, path, cmd, count(path) as cnt" 
//        			 + " group by user"
//        			 + " having cnt > 3 insert all events into outputStream;";

//        String query = "@info(name = 'query1') from eventStream[cmd == 'open' AND str:regexp(path, '/usr/data/[0-9]+/[0-9]+/[0-9]+')]#window.length(10)"
//        		+ " select user, path, cmd, count(path) as cnt" 
//        		+ " group by user"
//        		+ " having cnt > 3 insert all events into outputStream;";

//      String query = "@info(name = 'query1') from eventStream[cmd == 'open' AND str:regexp(path, '/usr/data/[0-9]+/[0-9]+/[0-9]+')]#window.timeBatch(1 sec)"
//				+ " select user, path, cmd, count(path) as cnt" 
//				+ " group by user"
//				+ " having cnt > 3 insert all events into outputStream;";

        String query = "@info(name = 'query1') from eventStream[cmd == 'open' AND str:regexp(path, '/usr/data/[0-9]+/[0-9]+/[0-9]+')]#window.lengthBatch(10)"
                + " select user, path, cmd, count(path) as cnt"
                + " group by user"
                + " having cnt > 3 insert all events into outputStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                alertCount++;
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("eventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"user", "/usr/data/0000/0000/0000", "open"});
        inputHandler.send(new Object[]{"user", "/usr/data/0000/0000/1111", "open"});
        inputHandler.send(new Object[]{"user", "/usr/data/0000/0000/2222", "open"});
        inputHandler.send(new Object[]{"user", "/usr/data/0000/0000/3333", "open"});

        inputHandler.send(new Object[]{"user", "/usr/data/1111/0000/0000", "open"});
        inputHandler.send(new Object[]{"user", "/usr/data/1111/0000/1111", "open"});

        inputHandler.send(new Object[]{"user", "/usr/data/2222/0000/0000", "open"});
        inputHandler.send(new Object[]{"user", "/usr/data/2222/0000/1111", "open"});
        inputHandler.send(new Object[]{"user", "/usr/data/2222/0000/2222", "open"});
        Thread.sleep(100);
        Assert.assertTrue(alertCount == 0);
        inputHandler.send(new Object[]{"user", "/usr/data/2222/0000/3333", "open"});
        Thread.sleep(100);
        Assert.assertTrue(alertCount == 1);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testSlideWindow2() throws Exception{
        alertCount = 0;
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream eventStream (timeStamp long, user string, path string, cmd string);";
        String query = "@info(name = 'query1') from eventStream[cmd == 'open' AND str:regexp(path, '/usr/data/[0-9]+/[0-9]+/[0-9]+')]#window.externalTime(timeStamp,1 sec)"
                + " select user, path, cmd, count(path) as cnt"
                + " group by user"
                + " having cnt > 3 insert all events into outputStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                alertCount++;
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("eventStream");
        executionPlanRuntime.start();
        long curTime = DateTimeUtil.humanDateToMilliseconds("2015-09-17 00:00:00,000");
        inputHandler.send(new Object[]{curTime, "user", "/usr/data/0000/0000/0000", "open"});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{curTime, "user", "/usr/data/0000/0000/1111", "open"});
        Thread.sleep(100);
        inputHandler.send(new Object[]{curTime, "user", "/usr/data/0000/0000/2222", "open"});
        Thread.sleep(100);
        inputHandler.send(new Object[]{curTime, "user", "/usr/data/0000/0000/3333", "open"});
        Thread.sleep(100);
        Assert.assertTrue(alertCount == 1);
        inputHandler.send(new Object[]{curTime, "user", "/usr/data/0000/0000/5555", "open"});
        Thread.sleep(100);
        Assert.assertTrue(alertCount == 2);
        executionPlanRuntime.shutdown();
    }
}