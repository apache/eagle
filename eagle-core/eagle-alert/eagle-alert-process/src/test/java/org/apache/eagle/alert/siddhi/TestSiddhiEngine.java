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

import java.lang.reflect.Field;

import org.apache.eagle.executor.AlertExecutor;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class TestSiddhiEngine {
    static final Logger log = LoggerFactory.getLogger(TestSiddhiEngine.class);
    int alertCount = 0;

    @Test
    public void TestStrContains() throws Exception {
        alertCount = 0;
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@config(async = 'true') " +
                "define stream typeStream (cmd string, src string, dst string) ;";
        String queryString = "" +
                "@info(name = 'query1') " +
                "from typeStream[(cmd == 'rename') and (src == '/tmp/pii') and (str:contains(dst,'/user/hdfs/.Trash/Current/tmp/pii')==true)] " +
                "select cmd, src, dst " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + queryString);

        QueryCallback callback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                alertCount++;
            }
        };
        executionPlanRuntime.addCallback("query1", callback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("typeStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"rename", "/tmp/pii", "/user/hdfs/.Trash/Current/tmp/pii"});
        Thread.sleep(100);
        Assert.assertTrue(alertCount == 1);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void TestRegexp() throws Exception {
        alertCount = 0;
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "@config(async = 'true') " +
                "define stream typeStream (str string, other string, num double) ;";
        String queryString = "" +
                "@info(name = 'query1') " +
                "from typeStream " +
                "select str as str1, other as other1 , num as num1, count(num) as number " +
                "having str:regexp(str1, '/usr/data/[0-9]+/[0-9]+/[0-9]+') == true " + 
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + queryString);

        QueryCallback callback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                alertCount++;
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        };
        
        executionPlanRuntime.addCallback("query1", callback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("typeStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"/usr/data/000/001/002", "other", 1.0});
        Thread.sleep(100);
        Assert.assertTrue(alertCount == 1);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void TestStrEqualsIgnoreCase() throws Exception {
        alertCount = 0;
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream typeStream (cmd string, src string, dst string) ;";
        String queryString = "" +
                "@info(name = 'query1') " +
                "from typeStream[(cmd == 'rename') and (src == '/tmp/pii') and (str:equalsIgnoreCase(dst,'/user/hdfs/.TRAsh/current/TMP/PII')==true)] " +
                "select cmd, src, dst " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + queryString);

        QueryCallback callback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                alertCount++;
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        };

        executionPlanRuntime.addCallback("query1", callback);

        Field field = QueryCallback.class.getDeclaredField("query");
        field.setAccessible(true);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("typeStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"rename", "/tmp/pii", "/user/HDFS/.Trash/Current/TMP/pii"}); // match case
        inputHandler.send(new Object[]{"rename", "/tmp/pii", "/user/HDFS/.Trash///Current/TMP/pii"}); //non-match case
        Thread.sleep(100);
        Assert.assertTrue(alertCount == 1);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void TestStrContainsIgnoreCase() throws Exception {
        alertCount = 0;
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream typeStream (cmd string, src string, dst string) ;";
        String queryString = "" +
                "@info(name = 'query1') " +
                "from typeStream[(cmd == 'rename') and (src == '/tmp/pii') and (str:containsIgnoreCase(dst,'.TRASH/CURRENT/tMp/pII')==true)] " +
                "select cmd, src, dst " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + queryString);

        QueryCallback callback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                alertCount++;
            }
        };

        executionPlanRuntime.addCallback("query1", callback);

        Field field = QueryCallback.class.getDeclaredField("query");
        field.setAccessible(true);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("typeStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"rename", "/tmp/pii", "/user/hdfs/.Trash/Current/TMP/pii"}); // match case
        inputHandler.send(new Object[]{"rename", "/tmp/pii", "/user/hdfs/.Trash///Current/TMP/pii"}); //non-match case
        Thread.sleep(100);
        Assert.assertTrue(alertCount == 1);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void TestRegexpIgnoreCase() throws Exception {
        alertCount = 0;
        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream typeStream (str string, other string, num double) ;";
        String queryString = "" +
                "@info(name = 'query1') " +
                "from typeStream " +
                "select str as str1, other as other1 , num as num1, count(num) as number " +
                "having str:regexpIgnoreCase(str1, '/usr/DATA/[0-9]+/[0-9]+/[0-9]+') == true " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + queryString);

        QueryCallback callback = new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                alertCount++;
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        };
        executionPlanRuntime.addCallback("query1", callback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("typeStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"/USR/data/000/001/002", "other", 1.0});
        Thread.sleep(100);
        Assert.assertTrue(alertCount == 1);
        executionPlanRuntime.shutdown();
    }
    
    @Test
    public void TestDataObject() throws Exception {
        alertCount = 0;
        SiddhiManager siddhiManager = new SiddhiManager();
        
        String cseEventStream = "" +
                "@config(async = 'true') " +
                "define stream typeStream (dataobj object, str string, first string) ;";
        String queryString = "" +
                "@info(name = 'query1') " +
                "from typeStream " +
                "select * " +
                "having str:regexp(str, '/usr/data/[0-9]+/[0-9]+/[0-9]+') == true " + 
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + queryString);

        QueryCallback callback = new QueryCallback() {
            @Override
           public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                alertCount++;
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        };

        executionPlanRuntime.addCallback("query1", callback);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("typeStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{new AlertExecutor(queryString, null, 0, 1, null, null), "/usr/data/000/001/002", "second"});
        Thread.sleep(100);
        Assert.assertTrue(alertCount == 1);
        executionPlanRuntime.shutdown();
    }
}
