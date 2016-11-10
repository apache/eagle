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

import java.util.Arrays;
import java.util.HashMap;

public class PolicyDefinitionTest {

    @Test
    public void testPolicyInnerDefinition() {
        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        def.setValue("test");
        def.setType("siddhi");
        def.setHandlerClass("setHandlerClass");
        def.setProperties(new HashMap<>());
        def.setOutputStreams(Arrays.asList("outputStream"));
        def.setInputStreams(Arrays.asList("inputStream"));
        Assert.assertEquals("{type=\"siddhi\",value=\"test\", inputStreams=\"[inputStream]\", outputStreams=\"[outputStream]\" }", def.toString());

        PolicyDefinition.Definition def1 = new PolicyDefinition.Definition();
        def1.setValue("test");
        def1.setType("siddhi");
        def1.setHandlerClass("setHandlerClass");
        def1.setProperties(new HashMap<>());
        def1.setOutputStreams(Arrays.asList("outputStream"));
        def1.setInputStreams(Arrays.asList("inputStream"));

        Assert.assertFalse(def == def1);
        Assert.assertTrue(def.equals(def1));
        Assert.assertTrue(def.hashCode() == def1.hashCode());

        def1.setInputStreams(Arrays.asList("inputStream1"));

        Assert.assertFalse(def.equals(def1));
        Assert.assertTrue(def.hashCode() == def1.hashCode());//problem  equals() and hashCode() be inconsistent

    }

    @Test
    public void testPolicyDefinition() {
        PolicyDefinition pd = new PolicyDefinition();
        PolicyDefinition.Definition def = new PolicyDefinition.Definition();
        def.setValue("test");
        def.setType("siddhi");
        def.setHandlerClass("setHandlerClass");
        def.setProperties(new HashMap<>());
        def.setOutputStreams(Arrays.asList("outputStream"));
        def.setInputStreams(Arrays.asList("inputStream"));
        pd.setDefinition(def);
        pd.setInputStreams(Arrays.asList("inputStream"));//confuse with PolicyDefinition.Definition InputStreams
        pd.setOutputStreams(Arrays.asList("outputStream"));//confuse with PolicyDefinition.Definition OutputStreams
        pd.setName("policyName");
        pd.setDescription(String.format("Test policy for stream %s", "streamName"));

        StreamPartition sp = new StreamPartition();
        sp.setStreamId("streamName");
        sp.setColumns(Arrays.asList("host"));
        sp.setType(StreamPartition.Type.GROUPBY);
        pd.addPartition(sp);
        Assert.assertEquals("{name=\"policyName\",definition={type=\"siddhi\",value=\"test\", inputStreams=\"[inputStream]\", outputStreams=\"[outputStream]\" }}", pd.toString());

        PolicyDefinition pd1 = new PolicyDefinition();
        PolicyDefinition.Definition def1 = new PolicyDefinition.Definition();
        def1.setValue("test");
        def1.setType("siddhi");
        def1.setHandlerClass("setHandlerClass");
        def1.setProperties(new HashMap<>());
        def1.setOutputStreams(Arrays.asList("outputStream"));
        def1.setInputStreams(Arrays.asList("inputStream"));
        pd1.setDefinition(def1);
        pd1.setInputStreams(Arrays.asList("inputStream"));//confuse with PolicyDefinition.Definition InputStreams
        pd1.setOutputStreams(Arrays.asList("outputStream"));//confuse with PolicyDefinition.Definition OutputStreams
        pd1.setName("policyName");
        pd1.setDescription(String.format("Test policy for stream %s", "streamName"));

        StreamPartition sp1 = new StreamPartition();
        sp1.setStreamId("streamName");
        sp1.setColumns(Arrays.asList("host"));
        sp1.setType(StreamPartition.Type.GROUPBY);
        pd1.addPartition(sp1);


        Assert.assertFalse(pd == pd1);
        Assert.assertTrue(pd.equals(pd1));
        Assert.assertTrue(pd.hashCode() == pd1.hashCode());
        sp1.setStreamId("streamName1");

        Assert.assertFalse(pd == pd1);
        Assert.assertFalse(pd.equals(pd1));
        Assert.assertFalse(pd.hashCode() == pd1.hashCode());

        sp1.setStreamId("streamName");
        def1.setOutputStreams(Arrays.asList("outputStream1"));

        Assert.assertFalse(pd == pd1);
        Assert.assertFalse(pd.equals(pd1));

        Assert.assertTrue(pd.hashCode() == pd1.hashCode());//problem  equals() and hashCode() be inconsistent

    }

    @Test
    public void testPolicyDefinitionEqualByPolicyStatus() {
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
