/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.engine.model;

import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

public class AlertPublishEventTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testAlertPublishEvent() {
        thrown.expect(NullPointerException.class);
        AlertStreamEvent alertStreamEvent = new AlertStreamEvent();
        AlertPublishEvent.createAlertPublishEvent(alertStreamEvent);
    }

    @Test
    public void testAlertPublishEvent1() {
        thrown.expect(NullPointerException.class);
        List<StreamColumn> streamColumns = new ArrayList<>();
        streamColumns.add(new StreamColumn.Builder().name("name").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("host").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("flag").type(StreamColumn.Type.BOOL).build());
        streamColumns.add(new StreamColumn.Builder().name("value").type(StreamColumn.Type.DOUBLE).build());
        streamColumns.add(new StreamColumn.Builder().name("data").type(StreamColumn.Type.LONG).build());
        streamColumns.add(new StreamColumn.Builder().name("salary").type(StreamColumn.Type.FLOAT).build());
        streamColumns.add(new StreamColumn.Builder().name("object").type(StreamColumn.Type.OBJECT).build());
        streamColumns.add(new StreamColumn.Builder().name("int").type(StreamColumn.Type.INT).build());

        StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setColumns(streamColumns);
        AlertStreamEvent alertStreamEvent = new AlertStreamEvent();
        alertStreamEvent.setSchema(streamDefinition);
        AlertPublishEvent.createAlertPublishEvent(alertStreamEvent);
    }

    @Test
    public void testAlertPublishEvent2() {
        List<StreamColumn> streamColumns = new ArrayList<>();
        streamColumns.add(new StreamColumn.Builder().name("name").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("host").type(StreamColumn.Type.STRING).build());
        streamColumns.add(new StreamColumn.Builder().name("flag").type(StreamColumn.Type.BOOL).build());
        streamColumns.add(new StreamColumn.Builder().name("value").type(StreamColumn.Type.DOUBLE).build());
        streamColumns.add(new StreamColumn.Builder().name("data").type(StreamColumn.Type.LONG).build());
        streamColumns.add(new StreamColumn.Builder().name("salary").type(StreamColumn.Type.FLOAT).build());
        streamColumns.add(new StreamColumn.Builder().name("object").type(StreamColumn.Type.OBJECT).build());
        streamColumns.add(new StreamColumn.Builder().name("int").type(StreamColumn.Type.INT).build());

        StreamDefinition streamDefinition = new StreamDefinition();
        streamDefinition.setColumns(streamColumns);
        AlertStreamEvent alertStreamEvent = new AlertStreamEvent();
        alertStreamEvent.setData(new Object[]{"namevalue", "hostvalue", "1", 10, 0.1, -0.2, "{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}", 1});
        alertStreamEvent.setSchema(streamDefinition);
        alertStreamEvent.setPolicyId("setPolicyId");
        alertStreamEvent.setCreatedTime(1234);
        alertStreamEvent.ensureAlertId();
        AlertPublishEvent alertPublishEvent = AlertPublishEvent.createAlertPublishEvent(alertStreamEvent);
        Assert.assertEquals(null, alertPublishEvent.getSiteId());
        Assert.assertTrue(alertPublishEvent.getAlertId() != null);
        Assert.assertEquals("setPolicyId", alertPublishEvent.getPolicyId());
        Assert.assertEquals(null, alertPublishEvent.getPolicyValue());
        Assert.assertEquals("{flag=1, data=0.1, name=namevalue, host=hostvalue, salary=-0.2, value=10, int=1, object={\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}}", alertPublishEvent.getAlertData().toString());
        Assert.assertEquals(1234, alertPublishEvent.getAlertTimestamp());
        Assert.assertEquals(null, alertPublishEvent.getAppIds());

        AlertPublishEvent alertPublishEvent1 = AlertPublishEvent.createAlertPublishEvent(alertStreamEvent);
        Assert.assertFalse(alertPublishEvent1 == alertPublishEvent);
        Assert.assertFalse(alertPublishEvent1.equals(alertPublishEvent));
        Assert.assertFalse(alertPublishEvent1.hashCode() == alertPublishEvent.hashCode());

        Map<String, Object> extraData = new HashMap<>();
        extraData.put(AlertPublishEvent.SITE_ID_KEY, "SITE_ID_KEY");
        extraData.put(AlertPublishEvent.POLICY_VALUE_KEY, "POLICY_VALUE_KEY");
        extraData.put(AlertPublishEvent.APP_IDS_KEY, Arrays.asList("appId1", "appId2"));
        alertStreamEvent.setExtraData(extraData);

        alertPublishEvent = AlertPublishEvent.createAlertPublishEvent(alertStreamEvent);
        Assert.assertEquals("SITE_ID_KEY", alertPublishEvent.getSiteId());
        Assert.assertTrue(alertPublishEvent.getAlertId() != null);
        Assert.assertEquals("setPolicyId", alertPublishEvent.getPolicyId());
        Assert.assertEquals("POLICY_VALUE_KEY", alertPublishEvent.getPolicyValue());
        Assert.assertEquals("{flag=1, data=0.1, name=namevalue, host=hostvalue, salary=-0.2, value=10, int=1, object={\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}}", alertPublishEvent.getAlertData().toString());
        Assert.assertEquals(1234, alertPublishEvent.getAlertTimestamp());
        Assert.assertEquals("appId1", alertPublishEvent.getAppIds().get(0));
        Assert.assertEquals("appId2", alertPublishEvent.getAppIds().get(1));

    }
}
