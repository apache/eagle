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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition.Definition;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class TestServiceTestAppWithZk extends AlertServiceTestBase {
    @Test
    public void testMain() throws Exception {
        System.setProperty("coordinator.zkConfig.zkQuorum", "localhost:" + getBindZkPort());
        Config config = ConfigFactory.load().getConfig("coordinator");
        // build dynamic policy loader
        String host = config.getString("metadataService.host");
        String context = config.getString("metadataService.context");
        IMetadataServiceClient client = new MetadataServiceClientImpl(host, getBindServerPort(), context);

        List<PolicyDefinition> policies = client.listPolicies();

        Assert.assertEquals(0, policies.size());

        PolicyDefinition def = new PolicyDefinition();
        def.setName("test-policy-1");
        def.setInputStreams(Collections.singletonList("testStreamDef"));
        def.setOutputStreams(Collections.singletonList("test-datasource-1"));
        def.setParallelismHint(5);
        def.setDefinition(new Definition());
        client.addPolicy(def);

        policies = client.listPolicies();

        Assert.assertEquals(1, policies.size());

        try {
            client.close();
        } catch (IOException e) {
            // ignore
        }
    }

}
