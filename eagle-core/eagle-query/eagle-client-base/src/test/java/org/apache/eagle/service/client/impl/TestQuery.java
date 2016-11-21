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
import org.apache.commons.lang.time.DateUtils;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.eagle.service.client.EagleServiceSingleEntityQueryRequest;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;

/**
 * @Since 11/15/16.
 */
public class TestQuery {

    @Test
    public void testQuery() throws Exception {
        EagleServiceConnector connector = mock(EagleServiceConnector.class);
        IEagleServiceClient client = new EagleServiceClientImpl(connector);
        String site = "sandbox";
        String query = "AuditService" + "[@serviceName=\"AlertDataSourceService\" AND @site=\"" + site + "\"]{*}";
        SearchRequestBuilder searchRequestBuilder = client.search().startRowkey("rowkey").startTime(0).endTime(10 * DateUtils.MILLIS_PER_DAY).pageSize(Integer.MAX_VALUE).filterIfMissing(true).metricName("metric").query(query);
        Field requestField = SearchRequestBuilder.class.getDeclaredField("request");
        requestField.setAccessible(true);
        EagleServiceSingleEntityQueryRequest request = (EagleServiceSingleEntityQueryRequest) requestField.get(searchRequestBuilder);
        String expected = "query=AuditService%5B%40serviceName%3D%22AlertDataSourceService%22+AND+%40site%3D%22sandbox%22%5D%7B*%7D&startRowkey=rowkey&pageSize=2147483647&startTime=1970-01-01%2000:00:00&endTime=1970-01-11%2000:00:00&treeAgg=false&metricName=metric&filterIfMissing=true";
        Assert.assertEquals(expected, request.getQueryParameterString());
    }
}


