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

/**
 * Since 7/8/16.
 */
public class TestAbsencePolicyHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TestAbsencePolicyHandler.class);
    private static final String inputStream = "testInputStream";
    private static final String outputStream = "testOutputStream";

    @Test
    public void test() throws Exception {
        test(buildPolicyDef_provided());
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
            Assert.assertEquals("host2", data[1]);
            LOG.info(e.toString());
        }
    }

    private PolicyDefinition buildPolicyDef_provided() {
        PolicyDefinition pd = new PolicyDefinition();
        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        def.setValue("1,jobID,job1,daily_rule,14:00:00,15:00:00");
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