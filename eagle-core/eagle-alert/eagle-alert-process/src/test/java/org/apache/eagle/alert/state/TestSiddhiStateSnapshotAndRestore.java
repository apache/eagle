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
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;

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
        Thread.sleep(100);
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

    @Ignore
    public void testLengthSlideWindow() throws Exception{
        String tmpdir = System.getProperty("java.io.tmpdir");
        System.out.println("temporary directory: " + tmpdir);

        String stateFile = tmpdir + "/siddhi-state-lengthslidewindow";
        ExecutionPlanRuntime executionPlanRuntime = setupRuntimeForLengthSlideWindow();
        executionPlanRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii_1"});
        executionPlanRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii_2"});
        executionPlanRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii_3"});
        executionPlanRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii_4"});
        Thread.sleep(100);
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

    private ExecutionPlanRuntime setupRuntimeForLengthSlideWindowWithGroupby(){
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream testStream (user string, cmd string);";
        String query = "@info(name = 'query1') from testStream#window.length(50) "
                + " select user, cmd, count(user) as cnt"
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

    @Ignore
    public void testLengthSlideWindowWithGroupby() throws Exception{
        String tmpdir = System.getProperty("java.io.tmpdir");
        System.out.println("temporary directory: " + tmpdir);

        String stateFile = tmpdir + "/siddhi-state-lengthslidewindowwithgroupby";
        ExecutionPlanRuntime executionPlanRuntime = setupRuntimeForLengthSlideWindowWithGroupby();
        executionPlanRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii_1"});
        executionPlanRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii_2"});
        executionPlanRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii_3"});
        executionPlanRuntime.getInputHandler("testStream").send(new Object[]{"rename", "/tmp/pii_4"});
        Thread.sleep(100);
        byte[] state = executionPlanRuntime.snapshot();
        int length = state.length;
        FileOutputStream output = new FileOutputStream(stateFile);
        output.write(state);
        output.close();
        executionPlanRuntime.shutdown();

        ExecutionPlanRuntime restoredRuntime = setupRuntimeForLengthSlideWindowWithGroupby();
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
        Thread.sleep(100);
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
    
    private ExecutionPlanRuntime setupRuntimeForExternalTimeSlideWindowWithGroupby(){
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream testStream (timeStamp long, user string, cmd string);";
        String query = "@info(name = 'query1') from testStream[cmd == 'open']#window.externalTime(timeStamp,30 sec)"
                + " select user, timeStamp, count(user) as cnt"
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
    public void testExternalTimeSlideWindowWithGroupby() throws Exception{
        String tmpdir = System.getProperty("java.io.tmpdir");
        System.out.println("temporary directory: " + tmpdir);

        String stateFile = tmpdir + "/siddhi-state-externaltimeslidewindow";
        ExecutionPlanRuntime executionPlanRuntime = setupRuntimeForExternalTimeSlideWindowWithGroupby();
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("testStream");
        long curTime = DateTimeUtil.humanDateToMilliseconds("2015-09-17 00:00:00,000");
        inputHandler.send(new Object[]{curTime, "user", "open"});
        inputHandler.send(new Object[]{curTime + 1000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 2000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 3000, "user", "open"});
        Thread.sleep(1000);

        byte[] state = executionPlanRuntime.snapshot();
        int length = state.length;
        FileOutputStream output = new FileOutputStream(stateFile);
        output.write(state);
        output.close();
        executionPlanRuntime.shutdown();

        ExecutionPlanRuntime restoredRuntime = setupRuntimeForExternalTimeSlideWindowWithGroupby();
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

    private ExecutionPlanRuntime setupRuntimeForExternalTimeSlideWindowWithGroupby_2(SiddhiManager siddhiManager){
        String cseEventStream = "define stream testStream (timeStamp long, user string, cmd string);";
        String query = "@info(name = 'query1') from testStream[cmd == 'open']#window.externalTime(timeStamp,300 sec)"
                + " select user, timeStamp, count(user) as cnt"
                + " group by user"
                + " insert all events into outputStream;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime("@Plan:name('testPlan') " + cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });
        return executionPlanRuntime;
    }

    @Test
    public void testExternalTimeSlideWindowWithGroupby_2() throws Exception{
        SiddhiManager siddhiManager = new SiddhiManager();
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        siddhiManager.setPersistenceStore(persistenceStore);

        ExecutionPlanRuntime executionPlanRuntime = setupRuntimeForExternalTimeSlideWindowWithGroupby_2(siddhiManager);
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("testStream");
        executionPlanRuntime.start();
        long curTime = DateTimeUtil.humanDateToMilliseconds("2015-09-17 00:00:00,000");
        inputHandler.send(new Object[]{curTime, "user", "open"});
        inputHandler.send(new Object[]{curTime + 1000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 2000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 3000, "user", "open"});
        Thread.sleep(100);
        executionPlanRuntime.persist();
        executionPlanRuntime.shutdown();
        ExecutionPlanRuntime restoredRuntime = setupRuntimeForExternalTimeSlideWindowWithGroupby_2(siddhiManager);
        inputHandler = restoredRuntime.getInputHandler("testStream");
        restoredRuntime.start();
        restoredRuntime.restoreLastRevision();
        inputHandler.send(new Object[]{curTime + 4000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 5000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 6000, "user", "open"});
        inputHandler.send(new Object[]{curTime + 7000, "user", "open"});
        Thread.sleep(1000);
        restoredRuntime.shutdown();
    }

    private ExecutionPlanRuntime setupRuntimeForInternalTimeSlideWindowWithGroupby(){
        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream testStream (user string, cmd string);";
        String query = "@info(name = 'query1') from testStream[cmd == 'open']#window.time(5 sec)"
                + " select user, count(user) as cnt"
                + " group by user"
                + " having cnt > 2"
                + " insert events into outputStream;";

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
    public void testInternalTimeSlideWindowWithGroupby() throws Exception{
        String tmpdir = System.getProperty("java.io.tmpdir");
        System.out.println("temporary directory: " + tmpdir);

        String stateFile = tmpdir + "/siddhi-state-internaltimeslidewindow";
        ExecutionPlanRuntime executionPlanRuntime = setupRuntimeForInternalTimeSlideWindowWithGroupby();
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("testStream");
        inputHandler.send(new Object[]{"user", "open"});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"user", "open"});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"user", "open"});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"user", "open"});

        byte[] state = executionPlanRuntime.snapshot();
        int length = state.length;
        FileOutputStream output = new FileOutputStream(stateFile);
        output.write(state);
        output.close();
        executionPlanRuntime.shutdown();

        ExecutionPlanRuntime restoredRuntime = setupRuntimeForInternalTimeSlideWindowWithGroupby();
        FileInputStream input = new FileInputStream(stateFile);
        byte[] restoredState = new byte[length];
        input.read(restoredState);
        restoredRuntime.restore(restoredState);
        inputHandler = restoredRuntime.getInputHandler("testStream");
        inputHandler.send(new Object[]{"user", "open"});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"user", "open"});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"user", "open"});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"user", "open"});
        Thread.sleep(1000);
        input.close();
        restoredRuntime.shutdown();
    }

    private int count;
    private boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    /**
     * Siddhi does not support external time window based snapshot
     * @throws InterruptedException
     */
    public void persistenceTest7() throws InterruptedException {
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);

        String executionPlan = "" +
                "@plan:name('Test') " +
                "" +
                "define stream StockStream (symbol string, price float, volume int, timestamp long);" +
                "" +
                "@info(name = 'query1')" +
                "from StockStream#window.externalTime(timestamp,30 sec) " +
                "select symbol, price, sum(volume) as totalVol, count(symbol) as cnt " +
                "group by symbol " +
                "insert into OutStream ";

        QueryCallback queryCallback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event inEvent : inEvents) {
                    count++;
                    Assert.assertTrue("IBM".equals(inEvent.getData(0)) || "WSO2".equals(inEvent.getData(0)));
                    if (count == 5) {
                        Assert.assertEquals(400l, inEvent.getData(2));
                    }
                    if (count == 6) {
                        Assert.assertEquals(200l, inEvent.getData(2));
                    }
                }
            }
        };

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();
        long currentTime = 0;

        inputHandler.send(new Object[]{"IBM", 75.1f, 100, currentTime + 1000});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.2f, 100, currentTime + 2000});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"IBM", 75.3f, 100, currentTime + 3000});

        Thread.sleep(500);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(3, count);

        //persisting
        Thread.sleep(500);
        executionPlanRuntime.persist();

        //restarting execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();
        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", queryCallback);
        inputHandler = executionPlanRuntime.getInputHandler("StockStream");
        executionPlanRuntime.start();

        //loading
        executionPlanRuntime.restoreLastRevision();

        inputHandler.send(new Object[]{"IBM", 75.4f, 100, currentTime + 4000});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"IBM", 75.5f, 100, currentTime + 5000});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 75.6f, 100, currentTime + 6000});

        //shutdown execution plan
        Thread.sleep(500);
        executionPlanRuntime.shutdown();

        Assert.assertEquals(count, 6);
        Assert.assertEquals(true, eventArrived);

    }
}
