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
package org.apache.eagle.storage.jdbc.criteria;

import org.apache.eagle.storage.exception.QueryCompileException;
import org.apache.eagle.storage.jdbc.conn.ConnectionManagerFactory;
import org.apache.eagle.storage.jdbc.criteria.impl.QueryCriteriaBuilder;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.eagle.storage.operation.RawQuery;
import org.apache.torque.criteria.Criteria;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 3/27/15
 */
public class TestQueryCriteriaBuilder {
    private final static Logger LOG = LoggerFactory.getLogger(TestQueryCriteriaBuilder.class);

    @Before
    public void setUp() throws Exception {
        ConnectionManagerFactory.getInstance();
    }

    //@Test
    public void testSimpleQueryBuilder() throws QueryCompileException {
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[@cluster=\"thecluster\" AND @field4 > 1000 ]{@field1,@field2, EXP{@field3/2}}");
        rawQuery.setStartTime("2015-01-06 01:40:02");
        rawQuery.setEndTime("2015-01-06 01:40:02");
        rawQuery.setPageSize(1000);
        CompiledQuery query = new CompiledQuery(rawQuery);

        QueryCriteriaBuilder criteriaBuilder = new QueryCriteriaBuilder(query,"test");
        Criteria criteria = criteriaBuilder.build();
        LOG.info(criteria.toString());
    }

    //@Test
    public void testSimpleQueryBuilder2() throws QueryCompileException {
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("TestTimeSeriesAPIEntity[@cluster=\"thecluster\" AND @field4 > 1000 OR @field5 < 10000 ]{@field1,@field2, EXP{@field3/2}}");
        rawQuery.setStartTime("2015-01-06 01:40:02");
        rawQuery.setEndTime("2015-01-06 01:40:02");
        rawQuery.setPageSize(1000);
        CompiledQuery query = new CompiledQuery(rawQuery);

        QueryCriteriaBuilder criteriaBuilder = new QueryCriteriaBuilder(query,"test");
        Criteria criteria = criteriaBuilder.build();
        LOG.info(criteria.toString());
    }

    //@Test
    public void testSimpleMetricQueryBuilder() throws QueryCompileException {
        RawQuery rawQuery = new RawQuery();
        rawQuery.setQuery("GenericMetricService[@cluster=\"thecluster\"]{@field1,@field2, EXP{@field3/2}}");
        rawQuery.setStartTime("2015-01-06 01:40:02");
        rawQuery.setEndTime("2015-01-06 01:40:02");
        rawQuery.setMetricName("metric.name.value");
        rawQuery.setPageSize(1000);
        CompiledQuery query = new CompiledQuery(rawQuery);

        QueryCriteriaBuilder criteriaBuilder = new QueryCriteriaBuilder(query,"test");
        Criteria criteria = criteriaBuilder.build();
        LOG.info(criteria.toString());
    }

    @Test
    public void test() {

    }
}
