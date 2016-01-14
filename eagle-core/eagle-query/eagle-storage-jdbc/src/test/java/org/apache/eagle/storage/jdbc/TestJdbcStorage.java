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
package org.apache.eagle.storage.jdbc;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.storage.DataStorageManager;
import org.apache.eagle.storage.exception.IllegalDataStorageTypeException;
import org.apache.eagle.storage.exception.QueryCompileException;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.eagle.storage.operation.RawQuery;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.storage.result.QueryResult;
import org.apache.eagle.common.DateTimeUtil;
import junit.framework.Assert;
import org.apache.commons.lang.time.StopWatch;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * create table unittest_testtsentity (uuid varchar(100), cluster varchar(10), datacenter varchar(10), field1 int, field2 int, field3 bigint, field4 bigint, field5 double, field6 double, field7 varchar(100), timestamp bigint)
 */
public class TestJdbcStorage {

    JdbcStorage storage;
    EntityDefinition entityDefinition;
    long baseTimestamp;
    final static Logger LOG = LoggerFactory.getLogger(TestJdbcStorage.class);

    @Before
    public void setUp() throws IOException, IllegalAccessException, InstantiationException, IllegalDataStorageTypeException {
        storage = (JdbcStorage) DataStorageManager.getDataStorageByEagleConfig();
        storage.init();
        entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);
        GregorianCalendar gc = new GregorianCalendar();
        gc.clear();
        gc.set(2014, 1, 6, 1, 40, 12);
        gc.setTimeZone(TimeZone.getTimeZone("UTC"));
        baseTimestamp = gc.getTime().getTime();
        System.out.println("timestamp:" + baseTimestamp);
    }

    @Test
    public void testReadBySimpleQuery() throws QueryCompileException, IOException {
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[]{*}");
        System.out.println(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithMilliseconds(baseTimestamp+2000));
        rawQuery.setPageSize(1000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult<TestTimeSeriesAPIEntity> result = storage.query(query, entityDefinition);
        Assert.assertNotNull(result);
    }

    @Test
    public void testReadByComplexQuery() throws QueryCompileException, IOException {
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[@cluster=\"c4ut\" AND @field4 > 1000 AND @field7 CONTAINS \"99404f47e309\" OR @datacenter =\"d4ut\" ]{@field1,@field2}");
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp + 2000));
        rawQuery.setPageSize(1000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        storage.query(query,entityDefinition);
    }

    @Test
    public void testWrite() throws IOException {
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<TestTimeSeriesAPIEntity>();

        int i= 0;
        while( i++ < 1000){
            entityList.add(newInstance());
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() > 0);
    }

    @Test
    public void testWriteAndRead() throws IOException, QueryCompileException {
        // record insert init time
        long startTime = System.currentTimeMillis();
        // Write 1000 entities
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<TestTimeSeriesAPIEntity>();
        int i= 0;
        while( i++ < 1000){
            entityList.add(newInstance());
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() >= 1000);
        // record insertion finish time
        long endTime = System.currentTimeMillis();

            // init read in time range [startTime, endTime)
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[]{*}");
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime+1));
        rawQuery.setPageSize(1000000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult queryResult = storage.query(query, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 1000);
    }

    @Test
    public void testWriteAndAggregation() throws IOException, QueryCompileException {
        // record insert init time
        long startTime = System.currentTimeMillis();
        // Write 1000 entities
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<TestTimeSeriesAPIEntity>();
        int i= 0;
        while( i++ < 1000){
            entityList.add(newInstance());
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() >= 1000);
        // record insertion finish time
        long endTime = System.currentTimeMillis();

        // init read in time range [startTime, endTime)
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[]<@cluster,@datacenter>{count,max(@field1),min(@field2),sum(@field3)}");
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime));
        rawQuery.setPageSize(1000000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult queryResult = storage.query(query, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 1);
    }

    @Test
    public void testWriteAndDelete() throws IOException, QueryCompileException {
        // record insert init time
        long startTime = System.currentTimeMillis();
        // Write 1000 entities
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<TestTimeSeriesAPIEntity>();
        int i= 0;
        while( i++ < 1000){
            entityList.add(newInstance());
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() >= 1000);
        // record insertion finish time
        long endTime = System.currentTimeMillis();

        // delete in time range [startTime, endTime)
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[]{*}");
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime));
        rawQuery.setPageSize(1000000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        ModifyResult<String> queryResult = storage.delete(query, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 1000);
    }

    @Test
    public void testWriteAndUpdate() throws IOException, QueryCompileException {
        // Write 1000 entities
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<TestTimeSeriesAPIEntity>();
        int i= 0;
        while( i++ < 1000){
            entityList.add(newInstance());
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() >= 1000);

        // record insertion finish time
        ModifyResult<String> queryResult = storage.update(entityList, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 1000);
    }

    /**
     * TODO: Investigate why writing performance becomes slower as records count increases
     *
     * 1) Wrote 100000 records in about 18820 ms for empty table
     * 2) Wrote 100000 records in about 35056 ms when 1M records in table
     *
     * @throws IOException
     */
    //@Test
    public void testWriterPerformance() throws IOException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<TestTimeSeriesAPIEntity>();
        int i= 0;
        while( i++ < 100000){
            entityList.add(newInstance());
            if(entityList.size()>=1000) {
                ModifyResult<String> result = storage.create(entityList, entityDefinition);
                Assert.assertNotNull(result);
                entityList.clear();
            }
        }
        stopWatch.stop();
        LOG.info("Wrote 100000 records in "+stopWatch.getTime()+" ms");
    }


    private TestTimeSeriesAPIEntity newInstance(){
        TestTimeSeriesAPIEntity instance = new TestTimeSeriesAPIEntity();
        instance.setField1(123);
        instance.setField2(234);
        instance.setField3(1231312312l);
        instance.setField4(12312312312l);
        instance.setField5(123123.12312);
        instance.setField6(-12312312.012);
        instance.setField7(UUID.randomUUID().toString());
        instance.setTags(new HashMap<String, String>() {{
            put("cluster", "c4ut");
            put("datacenter", "d4ut");
        }});
        instance.setTimestamp(baseTimestamp + 1000);
        return instance;
    }

    @Test
    public void test() {

    }
}
