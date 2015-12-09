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

import org.apache.eagle.common.DateTimeUtil;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * experiment Siddhi state snapshot and restore
 */
public class TestSiddhiStateSnapshotAndRestore {
    private ExecutionPlanRuntime setupRuntimeForSimple(){
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream testStream (cmd string, src string) ;";
        String queryString = "" +
                "@info(name = 'query1') " +
                "from testStream[(cmd == 'rename') and (src == '/tmp/pii')] " +
                "select cmd, src " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime("@Plan:name('testPlan') " + cseEventStream + queryString);

        QueryCallback callback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        };
        executionPlanRuntime.addCallback("query1", callback);
        executionPlanRuntime.start();
        return executionPlanRuntime;
    }

    @Test
    public void testSimpleSiddhiQuery() throws Exception{
        String tmpdir = System.getProperty("java.io.tmpdir");
        System.out.println("temporary directory: " + tmpdir);

        String stateFile = tmpdir + "/siddhi-state";
        ExecutionPlanRuntime executionPlanRuntime = setupRuntimeForSimple();
        executionPlanRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii"});
        byte[] state = executionPlanRuntime.snapshot();
        int length = state.length;
        FileOutputStream output = new FileOutputStream(stateFile);
        output.write(state);
        output.close();
        executionPlanRuntime.shutdown();

        ExecutionPlanRuntime restoredRuntime = setupRuntimeForSimple();
        FileInputStream input = new FileInputStream(stateFile);
        byte[] restoredState = new byte[length];
        input.read(restoredState);
        restoredRuntime.restore(restoredState);
        restoredRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii"});
        input.close();
        restoredRuntime.shutdown();
    }

    private ExecutionPlanRuntime setupRuntimeForLengthSlideWindow(){
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream testStream (user string, cmd string);";
        String query = "@info(name = 'query1') from testStream#window.length(3) "
                + " select *"
                + " insert all events into OutputStream";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime("@Plan:name('testPlan') " + cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        executionPlanRuntime.start();
        return executionPlanRuntime;
    }

    @Test
    public void testLengthSlideWindow() throws Exception{
        String tmpdir = System.getProperty("java.io.tmpdir");
        System.out.println("temporary directory: " + tmpdir);

        String stateFile = tmpdir + "/siddhi-state-lengthslidewindow";
        ExecutionPlanRuntime executionPlanRuntime = setupRuntimeForLengthSlideWindow();
        executionPlanRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii_1"});
        executionPlanRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii_2"});
        executionPlanRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii_3"});
        executionPlanRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii_4"});
        byte[] state = executionPlanRuntime.snapshot();
        int length = state.length;
        FileOutputStream output = new FileOutputStream(stateFile);
        output.write(state);
        output.close();
        executionPlanRuntime.shutdown();

        ExecutionPlanRuntime restoredRuntime = setupRuntimeForLengthSlideWindow();
        FileInputStream input = new FileInputStream(stateFile);
        byte[] restoredState = new byte[length];
        input.read(restoredState);
        restoredRuntime.restore(restoredState);
        restoredRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii_5"});
        input.close();
        restoredRuntime.shutdown();
    }

    private ExecutionPlanRuntime setupRuntimeForTimeSlideWindow(){
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream testStream (timeStamp long, user string, cmd string);";
        String query = "@info(name = 'query1') from testStream[cmd == 'open']#window.externalTime(timeStamp,3 sec)"
                + " select user, timeStamp " +
                "insert all events into outputStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime("@Plan:name('testPlan') " + cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
        executionPlanRuntime.start();
        return executionPlanRuntime;
    }

    @Test
    public void testTimeSlideWindow() throws Exception{
        String tmpdir = System.getProperty("java.io.tmpdir");
        System.out.println("temporary directory: " + tmpdir);

        String stateFile = tmpdir + "/siddhi-state-timeslidewindow";
        ExecutionPlanRuntime executionPlanRuntime = setupRuntimeForTimeSlideWindow();
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("testStream");
        long curTime = DateTimeUtil.humanDateToMilliseconds("2015-09-17 00:00:00,000");
        inputHandler.send(new Object[]{curTime, "user", "open"});
        inputHandler.send(new Object[]{curTime + 1000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 2000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 3000, "user", "open"});

        byte[] state = executionPlanRuntime.snapshot();
        int length = state.length;
        FileOutputStream output = new FileOutputStream(stateFile);
        output.write(state);
        output.close();
        executionPlanRuntime.shutdown();

        ExecutionPlanRuntime restoredRuntime = setupRuntimeForTimeSlideWindow();
        FileInputStream input = new FileInputStream(stateFile);
        byte[] restoredState = new byte[length];
        input.read(restoredState);
        restoredRuntime.restore(restoredState);
        inputHandler = restoredRuntime.getInputHandler("testStream");
        inputHandler.send(new Object[]{curTime + 4000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 5000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 6000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 7000, "user", "open"});
        Thread.sleep(1000);
        input.close();
        restoredRuntime.shutdown();
    }

    private ExecutionPlanRuntime setupRuntimeForTimeSlideWindowWithGroupby(){
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream testStream (timeStamp long, user string, cmd string);";
        String query = "@info(name = 'query1') from testStream[cmd == 'open']#window.externalTime(timeStamp,3 sec)"
                + " select user, timeStamp, count() as cnt"
                + " group by user"
                + " having cnt > 2"
               + " insert all events into outputStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime("@Plan:name('testPlan') " + cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
        executionPlanRuntime.start();
        return executionPlanRuntime;
    }

    @Test
    public void testTimeSlideWindowWithGroupby() throws Exception{
        String tmpdir = System.getProperty("java.io.tmpdir");
        System.out.println("temporary directory: " + tmpdir);

        String stateFile = tmpdir + "/siddhi-state-timeslidewindow";
        ExecutionPlanRuntime executionPlanRuntime = setupRuntimeForTimeSlideWindowWithGroupby();
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("testStream");
        long curTime = DateTimeUtil.humanDateToMilliseconds("2015-09-17 00:00:00,000");
        inputHandler.send(new Object[]{curTime, "user", "open"});
        inputHandler.send(new Object[]{curTime + 1000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 2000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 3000, "user", "open"});

        byte[] state = executionPlanRuntime.snapshot();
        int length = state.length;
        FileOutputStream output = new FileOutputStream(stateFile);
        output.write(state);
        output.close();
        executionPlanRuntime.shutdown();

        ExecutionPlanRuntime restoredRuntime = setupRuntimeForTimeSlideWindowWithGroupby();
        FileInputStream input = new FileInputStream(stateFile);
        byte[] restoredState = new byte[length];
        input.read(restoredState);
        restoredRuntime.restore(restoredState);
        inputHandler = restoredRuntime.getInputHandler("testStream");
        inputHandler.send(new Object[]{curTime + 4000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 5000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 6000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 7000, "user", "open"});
        Thread.sleep(1000);
        input.close();
        restoredRuntime.shutdown();
    }
}
