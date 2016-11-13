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
package org.apache.eagle.storage.operation;

import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.storage.exception.QueryCompileException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.GregorianCalendar;

/**
 * @Since 11/7/16.
 */
public class TestCompiledQuery {

    static EntityDefinition entityDefinition = null;

    final String queryStr = "TestTimeSeriesAPIEntity[@cluster=\"c4ut\"]{*}";
    final int pageSize = 1000;
    final long baseTimestamp = createTimeStamp(2017, 0, 11, 0, 0, 0);
    final String startTime = DateTimeUtil.millisecondsToHumanDateWithSeconds(baseTimestamp);
    final String endTime = DateTimeUtil.millisecondsToHumanDateWithMilliseconds(baseTimestamp + 2000);

    @BeforeClass
    public static void setUpOnce() throws Exception {
        entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class);
        entityDefinition.setTags(new String[] {"cluster", "datacenter", "random"});
    }

    static long createTimeStamp(int year, int month, int date, int hourOfDay, int minute, int second) {
        GregorianCalendar gc = new GregorianCalendar();
        gc.clear();
        gc.set(year, month, date, hourOfDay, minute, second);
        gc.setTimeZone(EagleConfigFactory.load().getTimeZone());
        return gc.getTime().getTime();
    }

    @Test
    public void testCreatedCompiledQuery() throws QueryCompileException, IOException {


        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery(queryStr);
        rawQuery.setStartTime(startTime);
        rawQuery.setEndTime(endTime);
        rawQuery.setPageSize(pageSize);

        CompiledQuery query = new CompiledQuery(rawQuery);

        Assert.assertEquals(baseTimestamp, query.getStartTime());
        Assert.assertEquals(baseTimestamp + 2000, query.getEndTime());
        Assert.assertEquals(rawQuery.isTreeAgg(), query.isHasAgg());
        Assert.assertEquals(rawQuery.isTimeSeries(), query.isTimeSeries());

        RawQuery raw2 = RawQuery.build().query(queryStr).startTime(startTime)
            .endTime(endTime).pageSize(pageSize).done();

        Assert.assertEquals(rawQuery.toString(), raw2.toString());
    }
}
