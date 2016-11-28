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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.eagle.log.base.taggedlog.EntityJsonModule;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.storage.exception.IllegalDataStorageTypeException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TestResourceUnmarshal {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeClass
    public static void setup() throws IllegalDataStorageTypeException, IOException {
        MAPPER.setFilters(TaggedLogAPIEntity.getFilterProvider());
        MAPPER.registerModule(new EntityJsonModule());
    }
    @Test
    public void testUnmarshalEntitiesByServie() throws NoSuchMethodException, InvocationTargetException, JsonProcessingException, IllegalAccessException, InstantiationException {

        TestTimeSeriesAPIEntity timeSeriesAPIEntity = new TestTimeSeriesAPIEntity();
        timeSeriesAPIEntity.setTimestamp(System.currentTimeMillis());
        timeSeriesAPIEntity.setField1(1);
        timeSeriesAPIEntity.setField2(2);
        timeSeriesAPIEntity.setField3(3);
        timeSeriesAPIEntity.setField4(4L);
        timeSeriesAPIEntity.setField5(5.0);
        timeSeriesAPIEntity.setField6(5.0);
        timeSeriesAPIEntity.setField7("7");
        timeSeriesAPIEntity.setTags(new HashMap<>());
        timeSeriesAPIEntity.getTags().put("cluster", "test4UT");
        timeSeriesAPIEntity.getTags().put("datacenter", "dc1");
        timeSeriesAPIEntity.getTags().put("index", "" + 1);
        timeSeriesAPIEntity.getTags().put("jobId", "job_" + timeSeriesAPIEntity.getTimestamp());

        List<TestTimeSeriesAPIEntity> timeSeriesAPIEntityList = new ArrayList<>();
        timeSeriesAPIEntityList.add(timeSeriesAPIEntity);

        GenericEntityServiceResource genericEntityServiceResource = new GenericEntityServiceResource();
        Method unmarshalEntitiesByServie = genericEntityServiceResource.getClass().getDeclaredMethod("unmarshalEntitiesByServie", InputStream.class, EntityDefinition.class);
        unmarshalEntitiesByServie.setAccessible(true);
        InputStream stream = new ByteArrayInputStream(MAPPER.writeValueAsString(timeSeriesAPIEntityList).getBytes(StandardCharsets.UTF_8));
        EntityDefinition ed = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);
        List<TestTimeSeriesAPIEntity> result = (List<TestTimeSeriesAPIEntity>) unmarshalEntitiesByServie.invoke(genericEntityServiceResource, stream, ed);

        Assert.assertEquals(timeSeriesAPIEntityList.get(0).getTimestamp(), result.get(0).getTimestamp());
        Assert.assertEquals(timeSeriesAPIEntityList.get(0).getField1(), result.get(0).getField1());
        Assert.assertEquals(timeSeriesAPIEntityList.get(0).getField2(), result.get(0).getField2());
        Assert.assertEquals(timeSeriesAPIEntityList.get(0).getField3(), result.get(0).getField3());
        Assert.assertEquals(timeSeriesAPIEntityList.get(0).getField4(), result.get(0).getField4());
        Assert.assertEquals(timeSeriesAPIEntityList.get(0).getField5(), result.get(0).getField5(), 0.1);
        Assert.assertEquals(timeSeriesAPIEntityList.get(0).getField6(), result.get(0).getField6());
        Assert.assertEquals(timeSeriesAPIEntityList.get(0).getField7(), result.get(0).getField7());


        Assert.assertEquals(timeSeriesAPIEntityList.get(0).getTags().get("cluster"), result.get(0).getTags().get("cluster"));
        Assert.assertEquals(timeSeriesAPIEntityList.get(0).getTags().get("datacenter"), result.get(0).getTags().get("datacenter"));
        Assert.assertEquals(timeSeriesAPIEntityList.get(0).getTags().get("index"), result.get(0).getTags().get("index"));
        Assert.assertEquals(timeSeriesAPIEntityList.get(0).getTags().get("jobId"), result.get(0).getTags().get("jobId"));

    }

    @Test
    public void testUnmarshalAsStringlist() throws NoSuchMethodException, JsonProcessingException, InvocationTargetException, IllegalAccessException {

        TestTimeSeriesAPIEntity timeSeriesAPIEntity = new TestTimeSeriesAPIEntity();
        timeSeriesAPIEntity.setTimestamp(1l);
        timeSeriesAPIEntity.setField1(1);
        timeSeriesAPIEntity.setField2(2);
        timeSeriesAPIEntity.setField3(3);
        timeSeriesAPIEntity.setField4(4L);
        timeSeriesAPIEntity.setField5(5.0);
        timeSeriesAPIEntity.setField6(5.0);
        timeSeriesAPIEntity.setField7("7");
        timeSeriesAPIEntity.setTags(new HashMap<>());
        timeSeriesAPIEntity.getTags().put("cluster", "test4UT");
        timeSeriesAPIEntity.getTags().put("datacenter", "dc1");
        timeSeriesAPIEntity.getTags().put("index", "" + 1);
        timeSeriesAPIEntity.getTags().put("jobId", "job_" + timeSeriesAPIEntity.getTimestamp());

        List<TestTimeSeriesAPIEntity> timeSeriesAPIEntityList = new ArrayList<>();
        timeSeriesAPIEntityList.add(timeSeriesAPIEntity);

        GenericEntityServiceResource genericEntityServiceResource = new GenericEntityServiceResource();
        Method unmarshalAsStringlist = genericEntityServiceResource.getClass().getDeclaredMethod("unmarshalAsStringlist", InputStream.class);
        unmarshalAsStringlist.setAccessible(true);

        InputStream stream = new ByteArrayInputStream(MAPPER.writeValueAsString(timeSeriesAPIEntityList).getBytes(StandardCharsets.UTF_8));
        List<String> result = (List<String>) unmarshalAsStringlist.invoke(genericEntityServiceResource, stream);

        Assert.assertEquals("[{, prefix, null, timestamp, 1, tags, {, cluster, test4UT, jobId, job_1, index, 1, datacenter, dc1, }, exp, null, encodedRowkey, null, serializeAlias, null, serializeVerbose, true, field1, 1, field2, 2, field3, 3, field4, 4, field5, 5.0, field6, 5.0, field7, 7, }]", result.toString());
    }
}
