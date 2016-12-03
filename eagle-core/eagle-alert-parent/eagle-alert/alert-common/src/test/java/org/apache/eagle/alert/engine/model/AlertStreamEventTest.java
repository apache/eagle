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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AlertStreamEventTest {

    @Test
    public void testAlertStreamEvent() {
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
        alertStreamEvent.setData(new Object[]{"namevalue", "hostvalue", "1", 10, 0.1, -0.2, "{\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}", 1});
        Assert.assertEquals(
                "Alert {stream=NULL,timestamp=1970-01-01 00:00:00,000,data={flag=1, data=0.1, name=namevalue, host=hostvalue, salary=-0.2, value=10, int=1, object={\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}}, policyId=null, createdBy=null, metaVersion=null}",
                alertStreamEvent.toString());
        Assert.assertEquals(
                "{flag=1, data=0.1, name=namevalue, host=hostvalue, salary=-0.2, value=10, int=1, object={\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}}",
                alertStreamEvent.getDataMap().toString());

        AlertStreamEvent alertStreamEvent1 = new AlertStreamEvent(alertStreamEvent);

        Assert.assertEquals(
                "Alert {stream=NULL,timestamp=1970-01-01 00:00:00,000,data={flag=1, data=0.1, name=namevalue, host=hostvalue, salary=-0.2, value=10, int=1, object={\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}}, policyId=null, createdBy=null, metaVersion=null}",
                alertStreamEvent1.toString());
        Assert.assertEquals(
                "{flag=1, data=0.1, name=namevalue, host=hostvalue, salary=-0.2, value=10, int=1, object={\"name\":\"heap.COMMITTED\", \"Value\":\"175636480\"}}",
                alertStreamEvent1.getDataMap().toString());

        Assert.assertFalse(alertStreamEvent1 == alertStreamEvent);
        Assert.assertTrue(alertStreamEvent1.equals(alertStreamEvent));
        Assert.assertTrue(alertStreamEvent1.hashCode() == alertStreamEvent.hashCode());
    }
}
