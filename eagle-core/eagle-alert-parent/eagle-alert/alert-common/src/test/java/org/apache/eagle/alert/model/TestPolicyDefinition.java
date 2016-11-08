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

package org.apache.eagle.alert.model;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.junit.Assert;
import org.junit.Test;

public class TestPolicyDefinition {

    @Test
    public void testEqual() {
        PolicyDefinition.Definition definition = new PolicyDefinition.Definition();
        PolicyDefinition policy1 = new PolicyDefinition();
        policy1.setName("policy1");
        policy1.setDefinition(definition);

        PolicyDefinition policy2 = new PolicyDefinition();
        policy2.setName("policy1");
        policy2.setPolicyStatus(PolicyDefinition.PolicyStatus.DISABLED);
        policy2.setDefinition(definition);

        PolicyDefinition policy3 = new PolicyDefinition();
        policy3.setName("policy1");
        policy3.setDefinition(definition);

        Assert.assertTrue(policy1.equals(policy3));
        Assert.assertFalse(policy1.equals(policy2));
    }
}
