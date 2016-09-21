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
package com.apache.eagle.service.app;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition.Definition;
import org.apache.eagle.service.metadata.resource.MetadataResource;

import com.sun.jersey.api.client.Client;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestAlertService {

    @Rule
    public final ResourceTestRule resources = ResourceTestRule.builder()
        .addResource(new MetadataResource())
        .build();

    @SuppressWarnings("unchecked")
    @Test
    public void testPolicyAddAndListRequest() throws Exception {
        Client client = resources.client();

        List<PolicyDefinition> policies =  resources.client().resource("/metadata/policies").get(List.class);

        Assert.assertEquals(0, policies.size());

        PolicyDefinition def = new PolicyDefinition();
        def.setName("test-policy-1");
        def.setInputStreams(Arrays.asList("testStreamDef"));
        def.setOutputStreams(Arrays.asList("test-datasource-1"));
        def.setParallelismHint(5);
        def.setDefinition(new Definition());
        resources.client().resource("/metadata/policies")
             .entity(def)
             .header("Content-Type","application/json")
             .post();
        policies = client.resource("/metadata/policies").get(List.class);
        Assert.assertEquals(1, policies.size());
    }
}
