/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.siddhiext;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.concurrent.Semaphore;

public class StringSubtractFunctionExtensionTest {
    private static final Logger LOG = LoggerFactory.getLogger(StringSubtractFunctionExtensionTest.class);

    @Test
    public void testStringSubtract() throws Exception {
        Semaphore semp = new Semaphore(1);
        String ql = " define stream log(timestamp long, switchLabel string, port string, message string); " +
                " from log select string:subtract(switchLabel, message) as alertKey insert into output; ";
        SiddhiManager manager = new SiddhiManager();
        ExecutionPlanRuntime runtime = manager.createExecutionPlanRuntime(ql);
        runtime.addCallback("output", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                Assert.assertTrue(events.length == 1);
                Assert.assertTrue(events[0].getData(0).toString().equals("a\nc\ne"));
                semp.release();
            }
        });

        runtime.start();

        InputHandler logInput = runtime.getInputHandler("log");
        semp.acquire();
        Event e = new Event();
        e.setTimestamp(System.currentTimeMillis());
        String ths = "[\"a\", \"b\", \"c\", \"d\", \"e\"]";
        String rhs = "[\"b\", \"d\"]";
        e.setData(new Object[] {System.currentTimeMillis(), ths, "port01", rhs});
        logInput.send(e);

        semp.acquire();
        runtime.shutdown();

    }

}
