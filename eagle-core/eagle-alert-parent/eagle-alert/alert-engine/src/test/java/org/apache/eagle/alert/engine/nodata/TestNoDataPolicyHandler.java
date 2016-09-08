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

import org.apache.eagle.alert.engine.Collector;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.PolicyHandlerContext;
import org.apache.eagle.alert.engine.evaluator.nodata.NoDataPolicyHandler;
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
 * Since 6/29/16.
 */
public class TestNoDataPolicyHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TestNoDataPolicyHandler.class);
    private static final String inputStream = "testInputStream";
    private static final String outputStream = "testOutputStream";

    @Test
    public void test() throws Exception {
        test(buildPolicyDef_provided());
        test(buildPolicyDef_dynamic());
    }

    @SuppressWarnings("unchecked")
    public void test(PolicyDefinition pd) throws Exception {
        Map<String, StreamDefinition> sds = new HashMap<>();
        StreamDefinition sd = buildStreamDef();
        sds.put("testInputStream", sd);
        NoDataPolicyHandler handler = new NoDataPolicyHandler(sds);

        PolicyHandlerContext context = new PolicyHandlerContext();
        context.setPolicyDefinition(pd);
        handler.prepare(new TestCollector(), context);

        handler.send(buildStreamEvt(0, "host1", 12.5));
        handler.send(buildStreamEvt(0, "host2", 12.6));
        handler.send(buildStreamEvt(100, "host1", 20.9));
        handler.send(buildStreamEvt(120, "host2", 22.1));
        handler.send(buildStreamEvt(4000, "host2", 22.1));
        handler.send(buildStreamEvt(50000, "host2", 22.1));
        handler.send(buildStreamEvt(60150, "host2", 22.3));
        handler.send(buildStreamEvt(60450, "host2", 22.9));
        handler.send(buildStreamEvt(75000, "host1", 41.6));
        handler.send(buildStreamEvt(85000, "host2", 45.6));
    }

    @SuppressWarnings("rawtypes")
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
        def.setValue("PT1M,provided,1,host,host1,host2");
        def.setType("nodataalert");
        pd.setDefinition(def);
        pd.setInputStreams(Arrays.asList(inputStream));
        pd.setOutputStreams(Arrays.asList(outputStream));
        pd.setName("nodataalert-test");
        return pd;
    }

    private PolicyDefinition buildPolicyDef_dynamic() {
        PolicyDefinition pd = new PolicyDefinition();
        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        def.setValue("PT1M,dynamic,1,host");
        def.setType("nodataalert");
        pd.setDefinition(def);
        pd.setInputStreams(Arrays.asList(inputStream));
        pd.setOutputStreams(Arrays.asList(outputStream));
        pd.setName("nodataalert-test");
        return pd;
    }

    private StreamDefinition buildStreamDef() {
        StreamDefinition sd = new StreamDefinition();
        StreamColumn tsColumn = new StreamColumn();
        tsColumn.setName("timestamp");
        tsColumn.setType(StreamColumn.Type.LONG);

        StreamColumn hostColumn = new StreamColumn();
        hostColumn.setName("host");
        hostColumn.setType(StreamColumn.Type.STRING);

        StreamColumn valueColumn = new StreamColumn();
        valueColumn.setName("value");
        valueColumn.setType(StreamColumn.Type.DOUBLE);

        sd.setColumns(Arrays.asList(tsColumn, hostColumn, valueColumn));
        sd.setDataSource("testDataSource");
        sd.setStreamId("testStreamId");
        return sd;
    }

    private StreamEvent buildStreamEvt(long ts, String host, double value) {
        StreamEvent e = new StreamEvent();
        e.setData(new Object[] {ts, host, value});
        e.setStreamId(inputStream);
        e.setTimestamp(ts);
        return e;
    }
}
