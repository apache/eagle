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
package org.apache.eagle.alert.engine.siddhi.extension;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

/**
 * @since Apr 1, 2016
 */
public class AttributeCollectAggregatorTest {

    private static final Logger logger = LoggerFactory.getLogger(AttributeCollectAggregatorTest.class);

    @Test
    public void test() throws Exception {
        String ql = "define stream s1(timestamp long, host string, type string);";
        ql += " from s1#window.externalTime(timestamp, 1 sec)";
        ql += " select eagle:collect(timestamp) as timestamps, eagle:collect(host) as hosts, type group by type insert into output;";

        SiddhiManager sm = new SiddhiManager();
        ExecutionPlanRuntime runtime = sm.createExecutionPlanRuntime(ql);

        InputHandler input = runtime.getInputHandler("s1");
        runtime.addCallback("output", new StreamCallback() {

            @Override
            public void receive(Event[] arg0) {
                logger.info("output event length:" + arg0.length);

                for (Event e : arg0) {
                    StringBuilder sb = new StringBuilder("\t - [").append(e.getData().length).append("]");
                    for (Object o : e.getData()) {
                        sb.append("," + o);
                    }
                    logger.info(sb.toString());
                }
                logger.info("===end===");
            }
        });
//        StreamDefinition definition = (StreamDefinition) runtime.getStreamDefinitionMap().get("output");

        runtime.start();

        Event[] events = generateEvents();
        for (Event e : events) {
            input.send(e);
        }

        Thread.sleep(1000);

    }

    private Event[] generateEvents() {
        List<Event> events = new LinkedList<Event>();

        Random r = new Random();
        Event e = null;
        long base = System.currentTimeMillis();
        {
            e = new Event(base, new Object[] {base, "host" + r.nextInt(), "nova"});
            base += 100;
            events.add(e);
            e = new Event(base, new Object[] {base, "host" + r.nextInt(), "nova"});
            base += 100;
            events.add(e);
            e = new Event(base, new Object[] {base, "host" + r.nextInt(), "nova"});
            base += 100;
            events.add(e);
        }

        {
            e = new Event(base, new Object[] {base, "host" + r.nextInt(), "neutron"});
            base += 100;
            events.add(e);
            e = new Event(base, new Object[] {base, "host" + r.nextInt(), "neutron"});
            base += 100;
            events.add(e);
            e = new Event(base, new Object[] {base, "host" + r.nextInt(), "neutron"});
            base += 100;
            events.add(e);
        }

        base += 10000;
        {
            e = new Event(base, new Object[] {base, "host" + r.nextInt(), "nova1"});
            base += 100;
            events.add(e);
            e = new Event(base, new Object[] {base, "host" + r.nextInt(), "nova1"});
            base += 100;
            events.add(e);
            e = new Event(base, new Object[] {base, "host" + r.nextInt(), "nova1"});
            base += 100;
            events.add(e);
        }

        {
            e = new Event(base, new Object[] {base, "host" + r.nextInt(), "neutron2"});
            base += 100;
            events.add(e);
            e = new Event(base, new Object[] {base, "host" + r.nextInt(), "neutron2"});
            base += 100;
            events.add(e);
            e = new Event(base, new Object[] {base, "host" + r.nextInt(), "neutron2"});
            base += 100;
            events.add(e);
        }
        base += 10000;
        e = new Event(base, new Object[] {base, "host" + r.nextInt(), "mq"});

        return events.toArray(new Event[0]);
    }

    @Test
    public void testQuery() {
        String ql = "define stream perfmon_input_stream_cpu ( host string,timestamp long,metric string,pool string,value double,colo string );";
        ql += "from perfmon_input_stream_cpu#window.length(3) select host, min(value) as min group by host having min>91.0 insert into perfmon_output_stream_cpu;";

        SiddhiManager sm = new SiddhiManager();
        sm.createExecutionPlanRuntime(ql);
    }
}
