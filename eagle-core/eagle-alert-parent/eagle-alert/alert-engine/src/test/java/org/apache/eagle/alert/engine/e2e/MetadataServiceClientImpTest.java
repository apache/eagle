/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.e2e;

import java.util.List;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.junit.Ignore;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class MetadataServiceClientImpTest {

    @Test
    @Ignore
    public void test() {
        System.out.println("loading metadatas...");
        try {
            System.setProperty("config.resource", "/application-integration.conf");
            ConfigFactory.invalidateCaches();
            Config config = ConfigFactory.load();
            loadMetadatas("/", config);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("loading metadatas done!");
    }

    private void loadMetadatas(String base, Config config) throws Exception {
        IMetadataServiceClient client = new MetadataServiceClientImpl(config);
        client.clear();

        List<Kafka2TupleMetadata> metadata = Integration1.loadEntities(base + "datasources.json", Kafka2TupleMetadata.class);
        client.addDataSources(metadata);

        List<PolicyDefinition> policies = Integration1.loadEntities(base + "policies.json", PolicyDefinition.class);
        client.addPolicies(policies);

        List<Publishment> pubs = Integration1.loadEntities(base + "publishments.json", Publishment.class);
        client.addPublishments(pubs);

        List<StreamDefinition> defs = Integration1.loadEntities(base + "streamdefinitions.json", StreamDefinition.class);
        client.addStreamDefinitions(defs);

        List<Topology> topos = Integration1.loadEntities(base + "topologies.json", Topology.class);
        client.addTopologies(topos);

        client.close();
    }

}
