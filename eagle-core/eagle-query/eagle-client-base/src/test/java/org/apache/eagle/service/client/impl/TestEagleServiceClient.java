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
package org.apache.eagle.service.client.impl;

import junit.framework.Assert;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.test.TestEntity;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.service.client.EagleServiceClientException;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;

/**
 * @Since 11/16/16.
 */
public class TestEagleServiceClient {

    @Test
    public void testBuildBathPath() {
        EagleServiceBaseClient eagleServiceBaseClient = new EagleServiceClientImpl("localhost", 9090, "admin", "secret");
        String expectedPath = "http://localhost:9090/rest";
        Assert.assertEquals(expectedPath, eagleServiceBaseClient.buildBathPath().toString());
    }

    @Test
    public void testMarshall() throws IOException {
        List<String> ids = new ArrayList<>();
        ids.add("eagle001");
        ids.add("eagle002");
        ids.add("eagle003");
        String expectedJson = "[\"eagle001\",\"eagle002\",\"eagle003\"]";
        Assert.assertEquals(expectedJson, EagleServiceBaseClient.marshall(ids));
        System.out.println(EagleServiceBaseClient.marshall(ids));
    }

    @Test(expected = EagleServiceClientException.class)
    public void testGroupEntitiesByServiceException() throws EagleServiceClientException {
        List<TaggedLogAPIEntity> entities = new ArrayList<>();
        TaggedLogAPIEntity testEntity1 = null;
        TaggedLogAPIEntity testEntity2 = new TestEntity();
        entities.add(testEntity1);
        entities.add(testEntity2);
        EagleServiceBaseClient eagleServiceBaseClient = new EagleServiceClientImpl("localhost", 9090, "admin", "secret");
        eagleServiceBaseClient.groupEntitiesByService(entities);
    }

    @Test
    public void testGroupEntitiesByService() throws EagleServiceClientException {
        List<TaggedLogAPIEntity> entities = new ArrayList<>();
        TaggedLogAPIEntity testEntity1 = new TestTimeSeriesAPIEntity();
        entities.add(testEntity1);
        EagleServiceBaseClient eagleServiceBaseClient = new EagleServiceClientImpl("localhost", 9090, "admin", "secret");
        Map<String,List<TaggedLogAPIEntity>> serviceEntityMap = eagleServiceBaseClient.groupEntitiesByService(entities);
        System.out.println(serviceEntityMap);
        Set<String> keySet = serviceEntityMap.keySet();
        for(Map.Entry<String, List<TaggedLogAPIEntity>> entry : serviceEntityMap.entrySet()) {
            Assert.assertEquals("TestTimeSeriesAPIEntity", entry.getKey());
            Assert.assertEquals(1, entry.getValue().size());
        }
    }

    @Test
    public void testGetServiceNameByService() throws EagleServiceClientException {
        EagleServiceConnector connector = mock(EagleServiceConnector.class);
        EagleServiceBaseClient client = new EagleServiceClientImpl(connector);
        String serviceName = client.getServiceNameByClass(TestTimeSeriesAPIEntity.class);
        Assert.assertEquals("TestTimeSeriesAPIEntity", serviceName);
    }

    @Test(expected = EagleServiceClientException.class)
    public void testGetServiceNameByServiceException() throws EagleServiceClientException {
        EagleServiceConnector connector = mock(EagleServiceConnector.class);
        EagleServiceBaseClient client = new EagleServiceClientImpl(connector);
        String serviceName = client.getServiceNameByClass(TestEntity.class);
        System.out.println(serviceName);
    }
}
