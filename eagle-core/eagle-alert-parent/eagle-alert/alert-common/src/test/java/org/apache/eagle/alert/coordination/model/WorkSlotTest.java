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

package org.apache.eagle.alert.coordination.model;

import org.junit.Assert;
import org.junit.Test;

public class WorkSlotTest {
    @Test
    public void testWorkSlot() {
        WorkSlot workSlot = new WorkSlot();
        Assert.assertEquals("(null:null)", workSlot.toString());
        Assert.assertEquals(null, workSlot.getBoltId());
        Assert.assertEquals(null, workSlot.getTopologyName());
        workSlot.setBoltId("setBoltId");
        workSlot.setTopologyName("setTopologyName");
        Assert.assertEquals("(setTopologyName:setBoltId)", workSlot.toString());
        Assert.assertEquals("setBoltId", workSlot.getBoltId());
        Assert.assertEquals("setTopologyName", workSlot.getTopologyName());

        WorkSlot workSlot1 = new WorkSlot("setTopologyName", "setBoltId");
        Assert.assertEquals("(setTopologyName:setBoltId)", workSlot1.toString());
        Assert.assertEquals("setBoltId", workSlot1.getBoltId());
        Assert.assertEquals("setTopologyName", workSlot1.getTopologyName());
        Assert.assertTrue(workSlot1.equals(workSlot));
        Assert.assertTrue(workSlot1.hashCode() == workSlot.hashCode());
    }
}
