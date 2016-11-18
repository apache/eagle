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

package org.apache.eagle.service.generic;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.jersey.api.client.Client;
import io.dropwizard.Configuration;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import javax.ws.rs.core.MediaType;

import static javax.ws.rs.core.HttpHeaders.*;


public class TestMetadataResource {

    private String restURL;
    private String restMetaURL;

    @ClassRule
    public static final DropwizardAppRule<Configuration> RULE =
        new DropwizardAppRule<>(MetadataResourceApp.class, null);


    private ObjectNode getMetadataResource(String url){
        return new Client().resource(restMetaURL + url )
            .header(ACCEPT, MediaType.APPLICATION_JSON)
            .header(CONTENT_TYPE, MediaType.APPLICATION_JSON)
            .get(ObjectNode.class);
    }

    @Before
    public void setUp()
    {
        restURL = "http://localhost:" + RULE.getLocalPort() + "/";
        restMetaURL = restURL +MetadataResource.PATH_META + "/";
    }
    @Test
    public void testListResource() {
        final ObjectNode clientResponse = getMetadataResource(MetadataResource.PATH_RESOURCE);
        Assert.assertEquals(2, clientResponse.size());
        Assert.assertEquals(restURL,clientResponse.get(MetadataResource.RESOURCE_BASE).textValue());
        Assert.assertEquals(1,clientResponse.get(MetadataResource.RESOURCE_RESOURCES).size());

        JsonNode node = clientResponse.get(MetadataResource.RESOURCE_RESOURCES).get(0);
        Assert.assertEquals(3,node.size());
        Assert.assertNotNull(node.get(MetadataResource.PATH_META));
        Assert.assertNotNull(node.get(MetadataResource.PATH_META+"/"+MetadataResource.PATH_SERVICE));
        Assert.assertNotNull(node.get(MetadataResource.PATH_META+"/"+MetadataResource.PATH_RESOURCE));
    }
    @Test
    public void testListService() {
        final ObjectNode clientResponse = getMetadataResource(MetadataResource.PATH_SERVICE);

        // EntityRepositoryScanner automatically scans entities, so we can't control the registered "services".
        // And hence we only verify summary here
        Assert.assertEquals(2, clientResponse.size());
        Assert.assertNotNull(clientResponse.get(MetadataResource.SERVICE_COUNT));
        Assert.assertNotNull(clientResponse.get(MetadataResource.SERVICE_SERVICES));
    }
    @Test
    public void testListMeta() {
        final ObjectNode clientResponse = getMetadataResource("");
        Assert.assertEquals(2, clientResponse.size());
        Assert.assertEquals(restMetaURL+MetadataResource.PATH_SERVICE,clientResponse.get(MetadataResource.PATH_SERVICE).textValue());
        Assert.assertEquals(restMetaURL+MetadataResource.PATH_RESOURCE,clientResponse.get(MetadataResource.PATH_RESOURCE).textValue());
    }
}
