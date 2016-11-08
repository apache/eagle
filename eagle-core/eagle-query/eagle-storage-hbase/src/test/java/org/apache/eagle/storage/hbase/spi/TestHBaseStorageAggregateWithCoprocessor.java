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
package org.apache.eagle.storage.hbase.spi;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.service.hbase.TestHBaseBase;
import org.apache.eagle.storage.DataStorage;
import org.apache.eagle.storage.DataStorageManager;
import org.apache.eagle.storage.exception.QueryCompileException;
import org.apache.eagle.storage.hbase.query.coprocessor.AggregateProtocolEndPoint;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.eagle.storage.operation.RawQuery;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.storage.result.QueryResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class TestHBaseStorageAggregateWithCoprocessor extends TestHBaseBase {
    static final Logger LOG = LoggerFactory.getLogger(TestHBaseStorageAggregateWithCoprocessor.class);
    EntityDefinition entityDefinition;
    DataStorage<String> storage;
    long baseTimestamp;

    // This is Bad, It will hide TestHBaseBase.setUpHBase!!!!
    @BeforeClass
    public static void setUpHBase() {
        System.setProperty("config.resource", "/application-co.conf");
        Configuration conf = new Configuration();
        conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, AggregateProtocolEndPoint.class.getName());
        TestHBaseBase.setupHBaseWithConfig(conf);
    }

    private TestTimeSeriesAPIEntity newInstance() {
        TestTimeSeriesAPIEntity instance = new TestTimeSeriesAPIEntity();
        instance.setField1(123);
        instance.setField2(234);
        instance.setField3(1231312312L);
        instance.setField4(12312312312L);
        instance.setField5(123123.12312);
        instance.setField6(-12312312.012);
        instance.setField7(UUID.randomUUID().toString());
        instance.setTags(new HashMap<String, String>() {
            {
                put("cluster", "c4ut");
                put("datacenter", "d4ut");
                put("random", UUID.randomUUID().toString());
            }
        });
        instance.setTimestamp(System.currentTimeMillis());
        return instance;
    }

    @Before
    public void setUp() throws Exception {
        entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);
        entityDefinition.setTags(new String[] {"cluster", "datacenter", "random"});

        storage = DataStorageManager.getDataStorageByEagleConfig();
        storage.init();
        GregorianCalendar gc = new GregorianCalendar();
        gc.clear();
        gc.set(2014, 1, 6, 1, 40, 12);
        gc.setTimeZone(TimeZone.getTimeZone("UTC"));
        baseTimestamp = gc.getTime().getTime();
        LOG.info("timestamp: {}", baseTimestamp);
    }


    @Test
    public void testWriteAndAggregation() throws IOException, QueryCompileException {
        // record insert init time
        final long startTime = System.currentTimeMillis();
        List<TestTimeSeriesAPIEntity> entityList = new ArrayList<>();
        int i = 0;
        while (i++ < 5) {
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
        rawQuery.setEndTime(DateTimeUtil.millisecondsToHumanDateWithSeconds(endTime + 1000));
        rawQuery.setPageSize(1000000);
        CompiledQuery query = new CompiledQuery(rawQuery);
        QueryResult queryResult = storage.query(query, entityDefinition);
        Assert.assertTrue(queryResult.getSize() >= 1);
    }
}
