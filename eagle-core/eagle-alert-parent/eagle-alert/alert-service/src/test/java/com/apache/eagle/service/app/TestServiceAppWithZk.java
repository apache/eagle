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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition.Definition;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.apache.eagle.alert.utils.ZookeeperEmbedded;
import org.apache.eagle.service.app.ServiceApp;
import org.junit.*;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestServiceAppWithZk {

    ZookeeperEmbedded zkEmbed;

    PrintStream oldStream;
    PrintStream newStream;
    ByteArrayOutputStream newStreamOutput;

    @Before
    public void setUp() throws Exception {
        // Create a stream to hold the output
        newStreamOutput = new ByteArrayOutputStream();
        newStream = new PrintStream(newStreamOutput);
        // IMPORTANT: Save the old System.out!
        oldStream = System.out;
        // Tell Java to use your special stream
        System.setOut(newStream);

        zkEmbed = new ZookeeperEmbedded(2181);
        zkEmbed.start();

        Thread.sleep(2000);

        new ServiceApp().run(new String[] {"server"});
    }

    @After
    public void tearDown() throws Exception {
        zkEmbed.shutdown();
    }

    @Test @Ignore
    public void testMain() throws Exception {
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e1) {
        }

        // Put things back
        System.out.flush();
        System.setOut(oldStream);

        BufferedReader br = new BufferedReader(new StringReader(newStreamOutput.toString()));
        List<String> logs = new ArrayList<String>();
        String line = null;
        while ((line = br.readLine()) != null) {
            logs.add(line);
        }

        System.out.println(Joiner.on("\n").join(logs));

        Assert.assertTrue(logs.stream().anyMatch((log) -> log.contains("this is leader node right now..")));
        Assert.assertTrue(logs.stream().anyMatch((log) -> log.contains("start coordinator background tasks..")));

        Config config = ConfigFactory.load().getConfig("coordinator");
        // build dynamic policy loader
        String host = config.getString("metadataService.host");
        int port = config.getInt("metadataService.port");
        String context = config.getString("metadataService.context");
        IMetadataServiceClient client = new MetadataServiceClientImpl(host, port, context);

        List<PolicyDefinition> policies = client.listPolicies();

        Assert.assertEquals(0, policies.size());

        PolicyDefinition def = new PolicyDefinition();
        def.setName("test-policy-1");
        def.setInputStreams(Arrays.asList("testStreamDef"));
        def.setOutputStreams(Arrays.asList("test-datasource-1"));
        def.setParallelismHint(5);
        def.setDefinition(new Definition());
        client.addPolicy(def);

        policies = client.listPolicies();

        Assert.assertEquals(1, policies.size());

        try {
            client.close();
        } catch (IOException e) {
        }
    }

}
