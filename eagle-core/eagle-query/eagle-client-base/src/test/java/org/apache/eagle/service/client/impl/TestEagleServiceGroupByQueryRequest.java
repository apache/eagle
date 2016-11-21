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
import org.apache.eagle.service.client.EagleServiceClientException;
import org.apache.eagle.service.client.EagleServiceGroupByQueryRequest;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

/**
 * @Since 11/18/16.
 */
public class TestEagleServiceGroupByQueryRequest {

    @Test
    public void testGetQueryString() throws EagleServiceClientException {
        EagleServiceGroupByQueryRequest request = new EagleServiceGroupByQueryRequest();
        request.setStartRowkey("rowkey");
        request.setStartTime("2016-11-18 00:00:00 000");
        request.setEndTime("2016-11-18 11:11:11 000");
        request.setMetricName("metric");
        request.setFilter("filter");
        request.setPageSize(Integer.MAX_VALUE);
        request.setIntervalMin(10);
        List<String> groupBys = new ArrayList<>();
        groupBys.add("field1");
        groupBys.add("field2");
        request.setGroupBys(groupBys);
        List<String> orderBys = new ArrayList<>();
        orderBys.add("field3");
        orderBys.add("field3");
        request.setOrderBys(orderBys);
        List<String> returns = new ArrayList<>();
        returns.add("field5");
        returns.add("field6");
        request.setReturns(returns);
        Assert.assertEquals("query=AuditService%5Bfilter%5D%3C%40field1%2C%40field2%3E%7Bfield5%2Cfield6%7D.%7Bfield3%2Cfield3%7D&startRowkey=rowkey&pageSize=2147483647&startTime=2016-11-18+00%3A00%3A00+000&endTime=2016-11-18+11%3A11%3A11+000&metricName=metric&timeSeries=true&intervalmin=10", request.getQueryParameterString("AuditService"));
    }
}
