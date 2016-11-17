package org.apache.eagle.storage.jdbc;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.storage.exception.QueryCompileException;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.eagle.storage.operation.RawQuery;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.storage.result.QueryResult;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class TestGenericMetricStorage extends JdbcStorageTestBase {

    EntityDefinition entityDefinition;
    Random random = new Random();
    String metricName = "unittest.metric.name";

    @Override
    public void setUp() throws Exception {
        entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(GenericMetricEntity.class);
        entityDefinition.setTags(new String[] {"site", "application"});
        super.setUp();
    }

    private GenericMetricEntity newMetric() {
        GenericMetricEntity instance = new GenericMetricEntity();
        instance.setPrefix(metricName);
        instance.setTags(new HashMap<String, String>() {{
            put("site", "unittest_site");
            put("application", "unittest_application");
        }});
        instance.setValue(new double[] {random.nextDouble()});
        instance.setTimestamp(System.currentTimeMillis());
        return instance;
    }

    @Test
    public void testWrite1000Metrics() throws InterruptedException, IOException {
        // Write 1000 entities
        List<GenericMetricEntity> entityList = new ArrayList<>();
        int i = 0;
        while (i++ < 1000) {
            entityList.add(newMetric());
            Thread.sleep(1);
        }
        ModifyResult<String> result = storage.create(entityList, entityDefinition);
        Assert.assertTrue(result.getSize() >= 1000);
    }

    @Test
    public void testSimpleRead() throws IOException, QueryCompileException, InterruptedException {
        // record insert init time
        long startTime = System.currentTimeMillis();
        // Write 1000 entities
        testWrite1000Metrics();
        // record insertion finish time
        long endTime = System.currentTimeMillis();
        // init read in time range [startTime, endTime)
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery(GenericMetricEntity.GENERIC_METRIC_SERVICE + "[]{*}");
        rawQuery.setMetricName(metricName);
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime + 1000));
        rawQuery.setPageSize(10000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult queryResult = storage.query(query, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 1000);
    }

    @Test
    public void testSimpleGroupAggregateRead() throws IOException, InterruptedException, QueryCompileException {
        long startTime = System.currentTimeMillis();
        testWrite1000Metrics();
        long endTime = System.currentTimeMillis();
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery(GenericMetricEntity.GENERIC_METRIC_SERVICE + "[@site=\"unittest_site\" AND @application=\"unittest_application\"]<@site>{sum(value)}");
        rawQuery.setMetricName(metricName);
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime + 1000));
        rawQuery.setPageSize(10000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult queryResult = storage.query(query, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 1);
    }


    @Test
    public void testTimeSeriesGroupAggregateRead() throws IOException, InterruptedException, QueryCompileException {
        long startTime = System.currentTimeMillis();
        testWrite1000Metrics();
        long endTime = System.currentTimeMillis();
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery(GenericMetricEntity.GENERIC_METRIC_SERVICE + "[@site=\"unittest_site\" AND @application=\"unittest_application\"]<@site>{sum(value)}");
        rawQuery.setMetricName(metricName);
        rawQuery.setTimeSeries(true);
        rawQuery.setIntervalmin(10);
        rawQuery.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(startTime));
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime + 10 * 60 * 1000));
        rawQuery.setPageSize(10000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult queryResult = storage.query(query, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 1);
    }
}