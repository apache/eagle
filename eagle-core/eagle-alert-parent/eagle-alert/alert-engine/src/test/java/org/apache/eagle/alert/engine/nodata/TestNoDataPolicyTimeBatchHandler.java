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
package org.apache.eagle.alert.engine.nodata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.eagle.alert.engine.Collector;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.PolicyHandlerContext;
import org.apache.eagle.alert.engine.evaluator.nodata.NoDataPolicyTimeBatchHandler;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestNoDataPolicyTimeBatchHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TestNoDataPolicyTimeBatchHandler.class);

    private static final String inputStream = "testInputStream";
    private static final String outputStream = "testOutputStream";

    @Before
    public void setup() {
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDynamic1() throws Exception {
        Map<String, StreamDefinition> sds = new HashMap<>();
        sds.put("testInputStream", buildStreamDef());
        sds.put("testOutputStream", buildOutputStreamDef());
        NoDataPolicyTimeBatchHandler handler = new NoDataPolicyTimeBatchHandler(sds);

        PolicyHandlerContext context = new PolicyHandlerContext();
        context.setPolicyDefinition(buildPolicyDef_dynamic());
        handler.prepare(new TestCollector(), context);

        long now = System.currentTimeMillis();

        handler.send(buildStreamEvt(now, "host1", 12.5));

        Thread.sleep(2000);

        handler.send(buildStreamEvt(now, "host2", 12.6));
        handler.send(buildStreamEvt(now, "host1", 20.9));
        handler.send(buildStreamEvt(now, "host2", 22.1));
        handler.send(buildStreamEvt(now, "host2", 22.1));

        Thread.sleep(5000);

        handler.send(buildStreamEvt(now, "host2", 22.1));
        handler.send(buildStreamEvt(now, "host2", 22.3));

        Thread.sleep(5000);

        handler.send(buildStreamEvt(now, "host2", 22.9));
        handler.send(buildStreamEvt(now, "host1", 41.6));
        handler.send(buildStreamEvt(now, "host2", 45.6));

        Thread.sleep(1000);
    }

    @SuppressWarnings("rawtypes")
    private static class TestCollector implements Collector {
        @Override
        public void emit(Object o) {
            AlertStreamEvent e = (AlertStreamEvent) o;
            Object[] data = e.getData();

            LOG.info("alert data: {}, {}", data[1], data[0]);
        }
    }

    private PolicyDefinition buildPolicyDef_dynamic() {
        PolicyDefinition pd = new PolicyDefinition();
        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        def.setValue("PT5S,dynamic");
        def.setType("nodataalert");
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("nodataColumnName", "host");
        def.setProperties(properties);
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
        sd.setStreamId("testInputStream");
        return sd;
    }

    private StreamDefinition buildOutputStreamDef() {
        StreamDefinition sd = new StreamDefinition();
        StreamColumn tsColumn = new StreamColumn();
        tsColumn.setName("timestamp");
        tsColumn.setType(StreamColumn.Type.LONG);

        StreamColumn hostColumn = new StreamColumn();
        hostColumn.setName("host");
        hostColumn.setType(StreamColumn.Type.STRING);

        StreamColumn valueColumn = new StreamColumn();
        valueColumn.setName("originalStreamName");
        valueColumn.setType(StreamColumn.Type.STRING);

        sd.setColumns(Arrays.asList(tsColumn, hostColumn, valueColumn));
        sd.setDataSource("testDataSource");
        sd.setStreamId("testOutputStream");
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
