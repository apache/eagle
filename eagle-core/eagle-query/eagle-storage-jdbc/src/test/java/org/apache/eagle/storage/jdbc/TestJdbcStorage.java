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

import org.junit.Assert;
import org.apache.commons.lang.time.StopWatch;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.storage.exception.QueryCompileException;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.eagle.storage.operation.RawQuery;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.storage.result.QueryResult;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class TestJdbcStorage extends JdbcStorageTestBase {
    EntityDefinition entityDefinition;
    @Override
    public void setUp() throws Exception {
        entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);
        entityDefinition.setTags(new String[]{"cluster","datacenter","random"});
        super.setUp();
    }

    @Test
    public void testReadBySimpleQuery() throws QueryCompileException, IOException {
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[@cluster=\"c4ut\"]{*}");
        System.out.println(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithMilliseconds(baseTimestamp+2000));
        rawQuery.setPageSize(1000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult<TestTimeSeriesAPIEntity> result = storage.query(query, entityDefinition);
        Assert.assertNotNull(result);
    }

    /**
     * To make sure not exception like Comparisons between 'LONG VARCHAR (UCS_BASIC)' and 'LONG VARCHAR (UCS_BASIC)' are not supported
     *
     * @throws QueryCompileException
     * @throws IOException
     */
    @Test
    public void testReadByStringTypedField() throws QueryCompileException, IOException {
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[@cluster=\"c4ut\" AND @field7 =\"some_field7_value\"]{*}");
        System.out.println(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithMilliseconds(baseTimestamp+2000));
        rawQuery.setPageSize(1000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult<TestTimeSeriesAPIEntity> result = storage.query(query, entityDefinition);
        Assert.assertNotNull(result);
    }

    @Test
    public void testReadByNotEqualCondition() throws QueryCompileException, IOException {
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[@cluster!=\"c4ut_not_found\" AND @field1 != 0]{*}");
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
        rawQuery.setQuery("TestTimeSeriesAPIEntity[@cluster=\"c4ut\" AND @field4 > 1000 OR @datacenter =\"d4ut\" ]{@field1,@field2}");
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp + 2000));
        rawQuery.setPageSize(1000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        storage.query(query,entityDefinition);
    }

    @Test
    public void testReadByComplexQueryWithLike() throws QueryCompileException, IOException {
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
        while( i++ < 5){
            TestTimeSeriesAPIEntity entity = newInstance();

            entityList.add(entity);
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() > 0);
    }

    @Test
    public void testWriteAndRead() throws IOException, QueryCompileException {
        // record insert init time
        long startTime = System.currentTimeMillis();
        // Write 1000 entities
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<>();
        int i= 0;
        while( i++ < 5){
            entityList.add(newInstance());
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() >= 5);
        // record insertion finish time
        long endTime = System.currentTimeMillis();

        // init read in time range [startTime, endTime)
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[]{*}");
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime+1000));
        rawQuery.setPageSize(10000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult queryResult = storage.query(query, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 5);
    }

    @Test
    public void testWriteAndAggregation() throws IOException, QueryCompileException {
        // record insert init time
        long startTime = System.currentTimeMillis();
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<>();
        int i= 0;
        while( i++ < 5){
            entityList.add(newInstance());
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() >= 5);
        // record insertion finish time
        long endTime = System.currentTimeMillis();

        // init read in time range [startTime, endTime)
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[]<@cluster,@datacenter>{count,max(@field1),min(@field2),sum(@field3)}");
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime+1000));
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
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<>();
        int i= 0;
        while( i++ < 5){
            entityList.add(newInstance());
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() >= 5);
        // record insertion finish time
        long endTime = System.currentTimeMillis();

        // delete in time range [startTime, endTime)
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[]{*}");
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime-1000));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime+1000));
        rawQuery.setPageSize(1000000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        ModifyResult<String> queryResult = storage.delete(query, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 5);
    }

    @Test
    public void testWriteAndUpdate() throws IOException, QueryCompileException {
        // Write 5 entities
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<>();
        int i= 0;
        while( i++ < 5){
            entityList.add(newInstance());
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() >= 5);

        // record insertion finish time
        ModifyResult<String> queryResult = storage.update(entityList, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 5);
    }

    /**
     * TODO: Investigate why writing performance becomes slower as records count increases
     *
     * 1) Wrote 100000 records in about 18820 ms for empty table
     * 2) Wrote 100000 records in about 35056 ms when 1M records in table
     *
     * @throws IOException
     */
    @Test @Ignore("Ignore performance auto testing")
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
            put("random",UUID.randomUUID().toString());
        }});
        instance.setTimestamp(System.currentTimeMillis());
        return instance;
    }
}
