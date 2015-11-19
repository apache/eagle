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
package org.apache.eagle.storage.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.storage.operation.CreateStatement;
import junit.framework.Assert;

import org.apache.eagle.storage.operation.QueryStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.service.hbase.TestHBaseBase;
import org.apache.eagle.storage.DataStorageManager;
import org.apache.eagle.storage.exception.IllegalDataStorageTypeException;
import org.apache.eagle.storage.operation.RawQuery;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.storage.result.QueryResult;
import org.apache.eagle.common.DateTimeUtil;

/**
 * @since 3/23/15
 */
public class TestHBaseStatement extends TestHBaseBase {

    EntityDefinition entityDefinition;

    @Before
    public void setUp() throws IOException, IllegalAccessException, InstantiationException, IllegalDataStorageTypeException {
        entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);
        hbase.createTable(entityDefinition.getTable(), entityDefinition.getColumnFamily());
    }

    @After
    public void cleanUp() throws IOException, IllegalAccessException, InstantiationException, IllegalDataStorageTypeException {
        entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);
        hbase.deleteTable(entityDefinition.getTable());
    }

    @Test
    public void testCreate() throws IllegalDataStorageTypeException, IOException {
        List<TestTimeSeriesAPIEntity> entities = new ArrayList<TestTimeSeriesAPIEntity>();
        TestTimeSeriesAPIEntity entity = new TestTimeSeriesAPIEntity();
        entity.setField1(1);
        entity.setField2(2);
        entity.setField3(10000000l);
        entity.setField4(10000000l);
        entity.setField5(0.12345678);
        entity.setTags(new HashMap<String, String>() {{
            put("cluster", "test");
            put("datacenter", "test");
            put("name","unit.test.name");
        }});

        entities.add(entity);

        CreateStatement createStatement = new CreateStatement(entities,"TestTimeSeriesAPIEntity");
        ModifyResult resultSet = createStatement.execute(DataStorageManager.newDataStorage("hbase"));

        Assert.assertEquals(1, resultSet.getIdentifiers().size());

        createStatement = new CreateStatement(entities,"TestTimeSeriesAPIEntity");
        resultSet = createStatement.execute(DataStorageManager.newDataStorage("hbase"));

        Assert.assertEquals(1, resultSet.getIdentifiers().size());
    }

    @Test
    public void testQuery() throws IllegalDataStorageTypeException, IOException {
        testCreate();
        RawQuery query = new RawQuery();
        query.setQuery("TestTimeSeriesAPIEntity[@cluster=\"test\" AND @datacenter = \"test\"]{*}");
        query.setPageSize(Integer.MAX_VALUE);
        query.setFilterIfMissing(false);
        query.setStartTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(0));
        query.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(System.currentTimeMillis()+25 * 3600 * 1000));
        QueryStatement queryStatement = new QueryStatement(query);
        QueryResult<?> entityResult = queryStatement.execute(DataStorageManager.newDataStorage("hbase"));
        assert entityResult != null;
    }
}