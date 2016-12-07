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
package org.apache.eagle.alert.engine.coordinator;

import org.junit.Assert;
import org.junit.Test;

public class PublishmentTypeTest {

    @Test
    public void testPublishmentType() {
        PublishmentType publishmentType = new PublishmentType();
        publishmentType.setName("KAFKA");
        publishmentType.setType("setClassName");
        publishmentType.setDescription("setDescription");

        PublishmentType publishmentType1 = new PublishmentType();
        publishmentType1.setName("KAFKA");
        publishmentType1.setType("setClassName");
        publishmentType1.setDescription("setDescription");

        Assert.assertFalse(publishmentType.equals(new String("")));
        Assert.assertFalse(publishmentType == publishmentType1);
        Assert.assertTrue(publishmentType.equals(publishmentType1));
        Assert.assertTrue(publishmentType.hashCode() == publishmentType1.hashCode());

        publishmentType1.setType("JMS");

        Assert.assertFalse(publishmentType.equals(publishmentType1));
        Assert.assertFalse(publishmentType.hashCode() == publishmentType1.hashCode());
    }
}
