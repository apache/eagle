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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.apache.eagle.log.base.taggedlog.EntityJsonModule;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.storage.DataStorage;
import org.apache.eagle.storage.DataStorageManager;
import org.apache.eagle.storage.exception.IllegalDataStorageTypeException;
import org.apache.eagle.storage.hbase.HBaseStorage;
import org.apache.eagle.storage.operation.UpdateStatement;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.storage.result.QueryResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.*;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DataStorageManager.class, HBaseConfiguration.class})
public class TestGenericEntityServiceResource {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private DataStorage dataStorage;
    private GenericEntityServiceResource genericEntityServiceResource = new GenericEntityServiceResource();

    @Rule
    public ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(genericEntityServiceResource).setMapper(MAPPER)
            .build();

    @BeforeClass
    public static void setupMapper() throws IllegalDataStorageTypeException, IOException {
        MAPPER.setFilters(TaggedLogAPIEntity.getFilterProvider());
        MAPPER.registerModule(new EntityJsonModule());
    }

    @Before
    public void setup() throws IllegalDataStorageTypeException {
        dataStorage = mock(HBaseStorage.class);
        mockStatic(DataStorageManager.class);
        mockStatic(HBaseConfiguration.class);
        when(DataStorageManager.getDataStorageByEagleConfig()).thenReturn(dataStorage);
        when(HBaseConfiguration.create()).thenReturn(new Configuration());
    }

    @Test
    public void testGenericEntityServiceResourceupdateDatabaseNullDataStorage() throws IllegalDataStorageTypeException {
        when(DataStorageManager.getDataStorageByEagleConfig()).thenReturn(null);
        GenericServiceAPIResponseEntity responseEntity = genericEntityServiceResource.updateDatabase(null);
        Assert.assertFalse(responseEntity.isSuccess());
        Assert.assertEquals(null, responseEntity.getMeta());
        Assert.assertEquals(null, responseEntity.getType());
        Assert.assertEquals(null, responseEntity.getObj());
        Assert.assertTrue(responseEntity.getException().startsWith("org.apache.eagle.storage.exception.IllegalDataStorageException: Data storage is null"));
    }

    @Test
    public void testGenericEntityServiceResourceupdateDatabaseFalse() throws IllegalDataStorageTypeException, IOException, IllegalAccessException, InstantiationException {
        TestTimeSeriesAPIEntity e = new TestTimeSeriesAPIEntity();
        List<TestTimeSeriesAPIEntity> entities = new ArrayList<>();
        entities.add(e);
        EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName("TestTimeSeriesAPIEntity");

        ModifyResult<String> modifyResult = new ModifyResult<>();
        modifyResult.setSuccess(false);
        when(dataStorage.update(entities, ed)).thenReturn(modifyResult);
        GenericServiceAPIResponseEntity responseEntity = genericEntityServiceResource.updateDatabase(new UpdateStatement(entities, "TestTimeSeriesAPIEntity"));
        Assert.assertFalse(responseEntity.isSuccess());
        Assert.assertEquals(null, responseEntity.getMeta());
        Assert.assertEquals(null, responseEntity.getType());
        Assert.assertEquals(null, responseEntity.getObj());
        Assert.assertEquals(null, responseEntity.getException());
        verify(dataStorage).update(entities, ed);
    }

    @Test
    public void testGenericEntityServiceResourceupdateDatabase() throws IllegalDataStorageTypeException, IOException, IllegalAccessException, InstantiationException {
        TestTimeSeriesAPIEntity e = new TestTimeSeriesAPIEntity();
        List<TestTimeSeriesAPIEntity> entities = new ArrayList<>();
        entities.add(e);
        EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName("TestTimeSeriesAPIEntity");

        ModifyResult<String> modifyResult = new ModifyResult<>();
        List<String> identifiers = new ArrayList<>();
        identifiers.add("test");
        modifyResult.setSuccess(true);
        modifyResult.setIdentifiers(identifiers);
        when(dataStorage.update(entities, ed)).thenReturn(modifyResult);
        GenericServiceAPIResponseEntity responseEntity = genericEntityServiceResource.updateDatabase(new UpdateStatement(entities, "TestTimeSeriesAPIEntity"));
        Assert.assertTrue(responseEntity.isSuccess());
        Assert.assertTrue(responseEntity.getMeta().toString().startsWith("{totalResults=1"));
        Assert.assertEquals(String.class, responseEntity.getType());
        Assert.assertEquals("test", responseEntity.getObj().get(0));
        Assert.assertEquals(null, responseEntity.getException());
        verify(dataStorage).update(entities, ed);
    }

    @Test
    public void testGenericEntityServiceResourceupdateDatabase1() throws IllegalDataStorageTypeException, IOException, IllegalAccessException, InstantiationException {
        TestTimeSeriesAPIEntity e = new TestTimeSeriesAPIEntity();
        List<TestTimeSeriesAPIEntity> entities = new ArrayList<>();
        entities.add(e);
        EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName("TestTimeSeriesAPIEntity");

        ModifyResult<String> modifyResult = new ModifyResult<>();
        List<String> identifiers = null;
        modifyResult.setSuccess(true);
        modifyResult.setIdentifiers(identifiers);
        when(dataStorage.update(entities, ed)).thenReturn(modifyResult);
        GenericServiceAPIResponseEntity responseEntity = genericEntityServiceResource.updateDatabase(new UpdateStatement(entities, "TestTimeSeriesAPIEntity"));
        Assert.assertTrue(responseEntity.isSuccess());
        Assert.assertTrue(responseEntity.getMeta().toString().startsWith("{totalResults=0"));
        Assert.assertEquals(null, responseEntity.getType());
        Assert.assertEquals(null, responseEntity.getObj());
        Assert.assertEquals(null, responseEntity.getException());
        verify(dataStorage).update(entities, ed);
    }


    @Test
    public void testGenericEntityServiceResourceSearchFalse() throws IllegalAccessException, InstantiationException, IOException {
        QueryResult<TestTimeSeriesAPIEntity> queryResult = new QueryResult<>();
        queryResult.setSuccess(false);
        List<String> rowkeys = new ArrayList<>();
        rowkeys.add("test");
        EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName("TestTimeSeriesAPIEntity");
        when(dataStorage.queryById(rowkeys, ed)).thenReturn(queryResult);
        GenericServiceAPIResponseEntity responseEntity = resources.client().resource("/entities/rowkey").queryParam("value", "test").queryParam("serviceName", "TestTimeSeriesAPIEntity").get(GenericServiceAPIResponseEntity.class);

        Assert.assertFalse(responseEntity.isSuccess());
        Assert.assertEquals(null, responseEntity.getMeta());
        Assert.assertEquals(null, responseEntity.getType());
        Assert.assertEquals(null, responseEntity.getObj());
        Assert.assertEquals(null, responseEntity.getException());
        verify(dataStorage).queryById(rowkeys, ed);
    }

    @Test
    public void testGenericEntityServiceResourceSearch() throws IllegalAccessException, InstantiationException, IOException {
        List<TestTimeSeriesAPIEntity> result = new ArrayList<>();
        TestTimeSeriesAPIEntity e = new TestTimeSeriesAPIEntity();
        long timestamp = System.currentTimeMillis();
        e.setTimestamp(timestamp);
        e.setField1(1);
        e.setField2(2);
        e.setField3(3);
        e.setField4(4L);
        e.setField5(5.0);
        e.setField6(5.0);
        e.setField7("7");
        e.setTags(new HashMap<>());
        e.getTags().put("cluster", "test4UT");
        e.getTags().put("datacenter", "dc1");
        e.getTags().put("index", "" + 1);
        e.getTags().put("jobId", "job_2");
        result.add(e);
        QueryResult<TestTimeSeriesAPIEntity> queryResult = new QueryResult<>();

        queryResult.setData(result);
        queryResult.setSuccess(true);
        queryResult.setSize(result.size());


        EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName("TestTimeSeriesAPIEntity");
        List<String> rowkeys = new ArrayList<>();
        rowkeys.add("test");
        when(dataStorage.queryById(rowkeys, ed)).thenReturn(queryResult);
        GenericServiceAPIResponseEntity responseEntity = resources.client().resource("/entities/rowkey").queryParam("value", "test").queryParam("serviceName", "TestTimeSeriesAPIEntity").get(GenericServiceAPIResponseEntity.class);

        Assert.assertTrue(responseEntity.isSuccess());
        Assert.assertTrue(responseEntity.getMeta().toString().startsWith("{firstTimestamp=null, totalResults=1, lastTimestamp=null, elapsedms="));
        Assert.assertEquals(null, responseEntity.getType());
        Assert.assertEquals("{prefix=null, timestamp=" + timestamp + ", tags={cluster=test4UT, jobId=job_2, index=1, datacenter=dc1}, exp=null, encodedRowkey=null, serializeAlias=null, serializeVerbose=true, field1=1, field2=2, field3=3, field4=4, field5=5.0, field6=5.0, field7=7}", responseEntity.getObj().get(0).toString());
        Assert.assertEquals(null, responseEntity.getException());
        verify(dataStorage).queryById(rowkeys, ed);
    }

    @Test
    public void testGenericEntityServiceResourceSearchNullServiceName() throws IllegalAccessException, InstantiationException, IOException {
        GenericServiceAPIResponseEntity responseEntity = resources.client().resource("/entities/rowkey").queryParam("value", "test").get(GenericServiceAPIResponseEntity.class);
        Assert.assertFalse(responseEntity.isSuccess());
        Assert.assertEquals(null, responseEntity.getMeta());
        Assert.assertEquals(null, responseEntity.getType());
        Assert.assertEquals(null, responseEntity.getObj());
        Assert.assertTrue(responseEntity.getException().startsWith("java.lang.Exception: java.lang.IllegalArgumentException: serviceName is null"));
        verify(dataStorage, never()).queryById(any(), any());
    }

    @Test
    public void testGenericEntityServiceResourceSearchNullDataStorage() throws IllegalAccessException, InstantiationException, IOException, IllegalDataStorageTypeException {
        when(DataStorageManager.getDataStorageByEagleConfig()).thenReturn(null);
        GenericServiceAPIResponseEntity responseEntity = resources.client().resource("/entities/rowkey").queryParam("value", "test").queryParam("serviceName", "TestTimeSeriesAPIEntity").get(GenericServiceAPIResponseEntity.class);
        Assert.assertFalse(responseEntity.isSuccess());
        Assert.assertEquals(null, responseEntity.getMeta());
        Assert.assertEquals(null, responseEntity.getType());
        Assert.assertEquals(null, responseEntity.getObj());
        Assert.assertTrue(responseEntity.getException().startsWith("java.lang.Exception: org.apache.eagle.storage.exception.IllegalDataStorageException: data storage is null"));
        verify(dataStorage, never()).queryById(any(), any());
    }
}
