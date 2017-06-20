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

package org.apache.eagle.metadata.utils;

import org.junit.Assert;
import org.junit.Test;

public class PolicyIdConversionsTest {

    @Test
    public void testGenerateUniquePolicyId() {
        Assert.assertEquals("mock_policy_test", PolicyIdConversions.generateUniquePolicyId("test", "mock_policy"));
    }

    @Test
    public void testParsePolicyId() {
        Assert.assertEquals("mock_policy", PolicyIdConversions.parsePolicyId("test", "mock_policy_test"));
    }

    @Test
    public void testParsePolicyId2() {
        Assert.assertEquals("mock_policy", PolicyIdConversions.parsePolicyId("test", "mock_policy"));
    }
}
