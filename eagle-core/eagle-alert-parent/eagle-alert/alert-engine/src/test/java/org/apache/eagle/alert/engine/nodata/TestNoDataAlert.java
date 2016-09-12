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
public class TestNoDataAlert {
    @Test
    public void test() throws Exception {
        String[] expectHosts = new String[] {"host_1", "host_2", "host_3", "host_4", "host_5", "host_6", "host_7", "host_8"};
//        String[] appearHosts = new String[]{"host_6","host_7","host_8"};
//        String[] noDataHosts = new String[]{"host_1","host_2","host_3","host_4","host_5"};

        ExecutionPlanRuntime runtime = new SiddhiManager().createExecutionPlanRuntime(
            "define stream appearStream (key string, src string);" +
                "define stream expectStream (key string, src string);" +
                "define table expectTable (key string, src string);" +
                "define trigger fiveSecTriggerStream at every 1 sec;" +
                "define trigger initAppearTriggerStream at 'start';" +
                "from expectStream insert into expectTable;" +
                "from fiveSecTriggerStream join expectTable insert into triggerExpectStream;" +
                "from initAppearTriggerStream join expectTable insert into initAppearStream;"
//                        "from triggerExpectStream as l left outer join appearStream#window.time(5 sec) as r on l.key == r.key select l.key as k1,r.key as k2 insert current events into joinStream;" +
//                        "from joinStream[k2 is null] select k1 insert current events into missingStream;"
        );

//        ExecutionPlanRuntime runtime = new SiddhiManager().createExecutionPlanRuntime(
//                "define stream appearStream (key string, src string);"+
//                        "define stream expectStream (key string, src string);"+
//                        "define table expectTable (key string, src string);"+
//                        "from expectStream insert into expectTable;"+
//                        "from appearStream#window.time(10 sec)  as l right outer join expectTable as r on l.key == r.key select r.key as k2, l.key as k1 insert current events into joinStream;" +
//                        "from joinStream[k1 is null] select k2 insert current events into missingStream;"
////                "from joinStream insert into missingStream;"
//
//        );

        runtime.addCallback("initAppearStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        runtime.start();
        for (String host : expectHosts) {
            runtime.getInputHandler("expectStream").send(System.currentTimeMillis(), new Object[] {host, "expectStream"});
        }

//        for(String host:appearHosts) {
//            runtime.getInputHandler("appearStream").send(System.currentTimeMillis(), new Object[]{host,"inStream"});
//        }

        Thread.sleep(5000);

//        for(String host:appearHosts) {
//            runtime.getInputHandler("appearStream").send(System.currentTimeMillis(), new Object[]{host,"inStream"});
//        }
//        Thread.sleep(10000);
    }

    /**
     * only alert when the successive 2 events has number of missing blocks changed
     * from every a = hadoopJmxMetricEventStream[ component=="namenode" and metric == "hadoop.namenode.dfs.missingblocks"] -> b = hadoopJmxMetricEventStream[b.component==a.component and b.metric==a.metric and b.host==a.host and convert(b.value, "long") > convert(a.value, "long") ] select b.metric as metric, b.host as host, b.value as newNumOfMissingBlocks, a.value as oldNumOfMissingBlocks, b.timestamp as timestamp, b.component as component, b.site as site insert into tmp;
     */
    @Test
    public void testMissingBlock() throws Exception {
        ExecutionPlanRuntime runtime = new SiddhiManager().createExecutionPlanRuntime(
            "define stream hadoopJmxMetricEventStream (component string, metric string, host string, site string, value double, timestamp long);" +
                "from every a = hadoopJmxMetricEventStream[ component==\"namenode\" and metric == \"hadoop.namenode.dfs.missingblocks\"] -> " +
                "b = hadoopJmxMetricEventStream[b.component==a.component and b.metric==a.metric and b.host==a.host and " +
                "convert(b.value, \"long\") > convert(a.value, \"long\") ] select b.metric as metric, b.host as host, " +
                "b.value as newNumOfMissingBlocks, a.value as oldNumOfMissingBlocks, b.timestamp as timestamp, b.component as component, " +
                "b.site as site insert into outputStream;"
        );

        runtime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        runtime.start();
        runtime.getInputHandler("hadoopJmxMetricEventStream").send(System.currentTimeMillis(), new Object[] {"namenode", "hadoop.namenode.dfs.missingblocks", "host1", "site1", 12.0, 123000L});
        runtime.getInputHandler("hadoopJmxMetricEventStream").send(System.currentTimeMillis(), new Object[] {"namenode", "hadoop.namenode.dfs.missingblocks", "host1", "site1", 13.0, 123100L});
        runtime.getInputHandler("hadoopJmxMetricEventStream").send(System.currentTimeMillis(), new Object[] {"namenode", "hadoop.namenode.dfs.missingblocks", "host1", "site1", 16.0, 123200L});


        Thread.sleep(5000);
    }
}