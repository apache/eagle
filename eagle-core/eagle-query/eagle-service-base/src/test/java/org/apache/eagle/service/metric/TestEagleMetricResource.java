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
package org.apache.eagle.service.metric;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.jersey.api.client.Client;
import io.dropwizard.Configuration;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericCreateAPIResponseEntity;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.service.generic.MetadataResource;
import org.apache.eagle.service.generic.MetadataResourceApp;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static org.junit.Assert.*;

/**
 * Created by luokun on 11/25/16.
 */
public class TestEagleMetricResource {

    private String restURL;
    private String requestJson;

    @ClassRule
    public static final DropwizardAppRule<Configuration> RULE =
        new DropwizardAppRule<>(EagleMetricResourceApp.class, null);


    private void getMetadataResource( ){
         new Client().resource(restURL )
            .header(ACCEPT, MediaType.APPLICATION_JSON)
            .header(CONTENT_TYPE, MediaType.APPLICATION_JSON)
            .post(GenericCreateAPIResponseEntity.class, requestJson);
    }

    @Before
    public void setUp() throws JsonProcessingException {
        restURL = "http://localhost:" + RULE.getLocalPort() + EagleMetricResource.METRIC_URL_PATH;
        List<GenericMetricEntity> entities = new ArrayList<GenericMetricEntity>();
        Map<String,String> tags = new HashMap<String, String>() {{
            put("cluster", "cluster4ut");
            put("datacenter", "datacenter4ut");
        }};
        for(int i=0;i<100;i++){
            GenericMetricEntity entity = new GenericMetricEntity();
            entity.setTimestamp(System.currentTimeMillis());
            entity.setTags(tags);
            entity.setValue(new double[]{1.234});
            entity.setPrefix("unit.test.metrics");
            entities.add(entity);
        }
        requestJson = TaggedLogAPIEntity.buildObjectMapper().writeValueAsString(entities);
    }

    @Test
    public void testCreateGenericMetricEntity() throws Exception {
        // getMetadataResource();
    }

}