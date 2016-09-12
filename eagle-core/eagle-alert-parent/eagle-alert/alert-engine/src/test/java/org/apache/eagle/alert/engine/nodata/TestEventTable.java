/**
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
package org.apache.eagle.alert.engine.nodata;

import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

/**
 * Since 6/27/16.
 */
public class TestEventTable {
    @Test
    public void test() throws Exception {
        ExecutionPlanRuntime runtime = new SiddhiManager().createExecutionPlanRuntime(
            "define stream expectStream (key string, src string);" +
                "define stream appearStream (key string, src string);" +
                "define table expectTable (key string, src string);" +
                "from expectStream insert into expectTable;" +
                "from appearStream[(expectTable.key==key) in expectTable] insert into outputStream;"
        );

        runtime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        runtime.start();
        runtime.getInputHandler("expectStream").send(System.currentTimeMillis(), new Object[] {"host1", "expectStream"});
        Thread.sleep(2000);
        runtime.getInputHandler("appearStream").send(System.currentTimeMillis(), new Object[] {"host2", "expectStream"});
        Thread.sleep(2000);
    }
}
