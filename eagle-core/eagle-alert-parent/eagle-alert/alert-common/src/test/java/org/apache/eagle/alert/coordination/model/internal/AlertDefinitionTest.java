/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.alert.coordination.model.internal;

import org.apache.eagle.alert.engine.coordinator.AlertDefinition;
import org.apache.eagle.alert.engine.coordinator.AlertSeverity;
import org.junit.Assert;
import org.junit.Test;

public class AlertDefinitionTest {

    @Test
    public void testEqual() {
        AlertDefinition ad1 = new AlertDefinition();
        ad1.setBody("body1");
        ad1.setCategory("email");
        ad1.setSeverity(AlertSeverity.CRITICAL);
        ad1.setSubject("");

        AlertDefinition ad2 = new AlertDefinition();
        ad2.setBody("body1");
        ad2.setCategory("email");
        ad2.setSeverity(AlertSeverity.CRITICAL);
        ad2.setSubject("");

        Assert.assertTrue(ad1.equals(ad2));

        ad1.setBody("body1");
        ad1.setCategory("email");
        ad1.setSeverity(AlertSeverity.FATAL);
        ad1.setSubject("");

        Assert.assertTrue(!ad1.equals(ad2));
    }
}
