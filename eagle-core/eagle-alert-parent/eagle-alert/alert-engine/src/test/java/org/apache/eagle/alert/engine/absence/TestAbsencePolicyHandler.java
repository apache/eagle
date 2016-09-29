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
package org.apache.eagle.alert.engine.absence;

import org.apache.eagle.alert.engine.Collector;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.PolicyHandlerContext;
import org.apache.eagle.alert.engine.evaluator.absence.AbsencePolicyHandler;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Since 7/8/16.
 */
public class TestAbsencePolicyHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TestAbsencePolicyHandler.class);
    private static final String inputStream = "testInputStream";
    private static final String outputStream = "testOutputStream";

    @Test
    public void test() throws Exception {
        String value = "1,jobID,job1,daily_rule,14:00:00,15:00:00";
        test(buildPolicyDef_provided(value));
    }

    @Test
    public void testHourlyRule() throws Exception {
        // "numOfFields, f1_name, f2_name, f1_value, f2_value, absenceWindowRuleType, startTime, endTime, interval"
        String value = "1,jobID,job1,hourly_rule,2016-09-09 12:00:00,2016-09-09 13:00:00,10:00:00";
        PolicyDefinition pd = buildPolicyDef_provided(value);

        Map<String, StreamDefinition> sds = new HashMap<>();
        StreamDefinition sd = buildStreamDef();
        sds.put("testInputStream", sd);
        AbsencePolicyHandler handler = new AbsencePolicyHandler(sds);

        PolicyHandlerContext context = new PolicyHandlerContext();
        context.setPolicyDefinition(pd);
        handler.prepare(new TestCollector(), context);

        // window=[startTime=3600000 (1970-01-01 01:00:00), endTime=7200000 (1970-01-01 02:00:00)]
        handler.send(buildStreamEvt(0, "job1", "running"));

        TimeUnit.SECONDS.sleep(1);
        
        // window=[startTime=39600000 (1970-01-01 11:00:00), endTime=43200000 (1970-01-01 12:00:00)]s
        handler.send(buildStreamEvt(7210000, "job1", "running"));
    }

    public void test(PolicyDefinition pd) throws Exception {
        Map<String, StreamDefinition> sds = new HashMap<>();
        StreamDefinition sd = buildStreamDef();
        sds.put("testInputStream", sd);
        AbsencePolicyHandler handler = new AbsencePolicyHandler(sds);

        PolicyHandlerContext context = new PolicyHandlerContext();
        context.setPolicyDefinition(pd);
        handler.prepare(new TestCollector(), context);

        handler.send(buildStreamEvt(0, "job1", "running"));
    }

    private static class TestCollector implements Collector {
        @Override
        public void emit(Object o) {
            AlertStreamEvent e = (AlertStreamEvent) o;
            Object[] data = e.getData();
            Assert.assertEquals("job1", data[1]);
            LOG.info(e.toString());
        }
    }

    private PolicyDefinition buildPolicyDef_provided(String value) {
        PolicyDefinition pd = new PolicyDefinition();
        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        def.setValue(value);
        def.setType("absencealert");
        pd.setDefinition(def);
        pd.setInputStreams(Arrays.asList(inputStream));
        pd.setOutputStreams(Arrays.asList(outputStream));
        pd.setName("absencealert-test");
        return pd;
    }

    private StreamDefinition buildStreamDef() {
        StreamDefinition sd = new StreamDefinition();
        StreamColumn tsColumn = new StreamColumn();
        tsColumn.setName("timestamp");
        tsColumn.setType(StreamColumn.Type.LONG);

        StreamColumn hostColumn = new StreamColumn();
        hostColumn.setName("jobID");
        hostColumn.setType(StreamColumn.Type.STRING);

        StreamColumn valueColumn = new StreamColumn();
        valueColumn.setName("status");
        valueColumn.setType(StreamColumn.Type.STRING);

        sd.setColumns(Arrays.asList(tsColumn, hostColumn, valueColumn));
        sd.setDataSource("testDataSource");
        sd.setStreamId("testStreamId");
        return sd;
    }

    private StreamEvent buildStreamEvt(long ts, String jobID, String status) {
        StreamEvent e = new StreamEvent();
        e.setData(new Object[] {ts, jobID, status});
        e.setStreamId(inputStream);
        e.setTimestamp(ts);
        return e;
    }
}