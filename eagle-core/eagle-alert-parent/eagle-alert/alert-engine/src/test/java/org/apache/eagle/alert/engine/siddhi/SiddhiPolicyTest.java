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
package org.apache.eagle.alert.engine.siddhi;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @since Jun 21, 2016
 */
public class SiddhiPolicyTest {

    private static final Logger LOG = LoggerFactory.getLogger(SiddhiPolicyTest.class);

    private String streams = " define stream syslog_stream("
        + "dims_facility string, "
        + "dims_severity string, "
        + "dims_hostname string, "
        + "dims_msgid string, "
        + "timestamp string, "
        + "conn string, "
        + "op string, "
        + "msgId string, "
        + "command string, "
        + "name string, "
        + "namespace string, "
        + "epochMillis long); ";
    private SiddhiManager sm;

    @Before
    public void setup() {
        sm = new SiddhiManager();
    }

    @After
    public void shutdown() {
        sm.shutdown();
    }

    @Test
    public void testPolicy_grpby() {
        String ql = " from syslog_stream#window.time(1min) select name, namespace, timestamp, dims_hostname, count(*) as abortCount group by dims_hostname insert into syslog_severity_check_output; ";
        StreamCallback sc = new StreamCallback() {
            @Override
            public void receive(Event[] arg0) {

            }

            ;
        };

        String executionPlan = streams + ql;
        ExecutionPlanRuntime runtime = sm.createExecutionPlanRuntime(executionPlan);
        runtime.addCallback("syslog_severity_check_output", sc);
        runtime.start();
    }

    @Ignore
    @Test
    public void testPolicy_agg() throws Exception {
        String sql = " from syslog_stream#window.time(1min) select "
            + "name, "
            + "namespace, "
            + "timestamp, "
            + "dims_hostname, "
            + "count(*) as abortCount "
            + "group by dims_hostname "
            + "having abortCount > 3 insert into syslog_severity_check_output; ";

        final AtomicBoolean checked = new AtomicBoolean(false);
        StreamCallback sc = new StreamCallback() {
            @Override
            public void receive(Event[] arg0) {
                checked.set(true);
                LOG.info("event array size: " + arg0.length);
                Set<String> hosts = new HashSet<String>();
                for (Event e : arg0) {
                    hosts.add((String) e.getData()[3]);
                }

                LOG.info(" grouped hosts : " + hosts);
                Assert.assertTrue(hosts.contains("HOSTNAME-" + 0));
                Assert.assertTrue(hosts.contains("HOSTNAME-" + 1));
                Assert.assertTrue(hosts.contains("HOSTNAME-" + 2));
                Assert.assertFalse(hosts.contains("HOSTNAME-" + 3));
            }

            ;
        };

        String executionPlan = streams + sql;
        ExecutionPlanRuntime runtime = sm.createExecutionPlanRuntime(executionPlan);
        runtime.addCallback("syslog_severity_check_output", sc);
        runtime.start();
        InputHandler handler = runtime.getInputHandler("syslog_stream");

        sendInput(handler);

        Thread.sleep(1000);

        Assert.assertTrue(checked.get());

        runtime.shutdown();
    }

    /*
        + "dims_facility string, "
        + "dims_severity string, "
        + "dims_hostname string, "
        + "dims_msgid string, "
        + "timestamp string, "
        + "conn string, "
        + "op string, "
        + "msgId string, "
        + "command string, "
        + "name string, "
        + "namespace string, "
        + "epochMillis long)
     */
    private void sendInput(InputHandler handler) throws Exception {
        int length = 15;
        Event[] events = new Event[length];
        for (int i = 0; i < length; i++) {
            Event e = new Event(12);
            e.setTimestamp(System.currentTimeMillis());
            e.setData(new Object[] {"facitliy", "SEVERITY_EMERG", "HOSTNAME-" + i % 4, "MSGID-...", "Timestamp", "conn-sss", "op-msg-Abort", "msgId..", "command-...", "name-", "namespace", System.currentTimeMillis()});

            events[i] = e;
        }

        handler.send(events);

        Thread.sleep(61 * 1000);

        Event e = new Event(12);
        e.setTimestamp(System.currentTimeMillis());
        e.setData(new Object[] {"facitliy", "SEVERITY_EMERG", "HOSTNAME-" + 11, "MSGID-...", "Timestamp", "conn-sss", "op-msg", "msgId..", "command-...", "name-", "namespace", System.currentTimeMillis()});
        handler.send(e);
    }

    @Ignore
    @Test
    public void testPolicy_regex() throws Exception {
        String sql = " from syslog_stream[regex:find(\"Abort\", op)]#window.time(1min) select timestamp, dims_hostname, count(*) as abortCount group by dims_hostname insert into syslog_severity_check_output; ";

        AtomicBoolean checked = new AtomicBoolean();
        StreamCallback sc = new StreamCallback() {
            @Override
            public void receive(Event[] arg0) {
                checked.set(true);
            }

            ;
        };

        String executionPlan = streams + sql;
        ExecutionPlanRuntime runtime = sm.createExecutionPlanRuntime(executionPlan);
        runtime.addCallback("syslog_severity_check_output", sc);
        runtime.start();

        InputHandler handler = runtime.getInputHandler("syslog_stream");

        sendInput(handler);

        Thread.sleep(1000);

        Assert.assertTrue(checked.get());

        runtime.shutdown();
    }

    @Ignore
    @Test
    public void testPolicy_seq() throws Exception {
        String sql = ""
            + " from every e1=syslog_stream[regex:find(\"UPDOWN\", op)] -> "
            + " e2=syslog_stream[dims_hostname == e1.dims_hostname and regex:find(\"Abort\", op)] within 1 min "
            + " select e1.timestamp as timestamp, e1.op as a_op, e2.op as b_op "
            + " insert into syslog_severity_check_output; ";

        AtomicBoolean checked = new AtomicBoolean();
        StreamCallback sc = new StreamCallback() {
            @Override
            public void receive(Event[] arg0) {
                checked.set(true);
            }

            ;
        };

        String executionPlan = streams + sql;
        ExecutionPlanRuntime runtime = sm.createExecutionPlanRuntime(executionPlan);
        runtime.addCallback("syslog_severity_check_output", sc);
        runtime.start();
        InputHandler handler = runtime.getInputHandler("syslog_stream");

        sendPatternInput(handler);

        Thread.sleep(1000);
        Assert.assertTrue(checked.get());

        runtime.shutdown();
    }

    private void sendPatternInput(InputHandler handler) throws Exception {
        // validate one
        Event e = new Event(12);
        e.setTimestamp(System.currentTimeMillis());
        e.setData(new Object[] {"facitliy", "SEVERITY_EMERG", "HOSTNAME-" + 0, "MSGID-...", "Timestamp", "conn-sss", "op-msg-UPDOWN", "msgId..", "command-...", "name-", "namespace", System.currentTimeMillis()});

        e = new Event(12);
        e.setTimestamp(System.currentTimeMillis());
        e.setData(new Object[] {"facitliy", "SEVERITY_EMERG", "HOSTNAME-" + 0, "MSGID-...", "Timestamp", "conn-sss", "op-msg-nothing", "msgId..", "command-...", "name-", "namespace", System.currentTimeMillis()});

        e = new Event(12);
        e.setTimestamp(System.currentTimeMillis());
        e.setData(new Object[] {"facitliy", "SEVERITY_EMERG", "HOSTNAME-" + 0, "MSGID-...", "Timestamp", "conn-sss", "op-msg-Abort", "msgId..", "command-...", "name-", "namespace", System.currentTimeMillis()});

        Thread.sleep(61 * 1000);

        e = new Event(12);
        e.setTimestamp(System.currentTimeMillis());
        e.setData(new Object[] {"facitliy", "SEVERITY_EMERG", "HOSTNAME-" + 11, "MSGID-...", "Timestamp", "conn-sss", "op-msg", "msgId..", "command-...", "name-", "namespace", System.currentTimeMillis()});
        handler.send(e);
    }


    @Test
    public void testStrConcat() throws Exception {
        String ql = " define stream log(timestamp long, switchLabel string, port string, message string); " +
            " from log select timestamp, str:concat(switchLabel, '===', port) as alertKey, message insert into output; ";
        SiddhiManager manager = new SiddhiManager();
        ExecutionPlanRuntime runtime = manager.createExecutionPlanRuntime(ql);
        runtime.addCallback("output", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        runtime.start();

        InputHandler logInput = runtime.getInputHandler("log");

        Event e = new Event();
        e.setTimestamp(System.currentTimeMillis());
        e.setData(new Object[] {System.currentTimeMillis(), "switch-ra-slc-01", "port01", "log-message...."});
        logInput.send(e);

        Thread.sleep(1000);
        runtime.shutdown();

    }

}
