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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * @Since 11/17/16.
 */
public class TestDeleteRequestBuilder {

    @Test
    public void testDeleteRequestByIds() throws Exception {
        EagleServiceConnector connector = mock(EagleServiceConnector.class);
        IEagleServiceClient client = new EagleServiceClientImpl(connector);
        List<String> ids = new ArrayList<>();
        ids.add("id1");
        ids.add("id2");
        ids.add("id3");
        DeleteRequestBuilder deleteRequestBuilder = client.delete().startRowkey("rowkey").startTime(1479369296000L).endTime(1479369296000L + DateUtils.MILLIS_PER_DAY).pageSize(Integer.MAX_VALUE).filterIfMissing(true).
            metricName("metric").treeAgg(false).byId(ids);
        Field deleteRequestField = DeleteRequestBuilder.class.getDeclaredField("request");
        Field idsField = DeleteRequestBuilder.class.getDeclaredField("deleteIds");
        deleteRequestField.setAccessible(true);
        idsField.setAccessible(true);
        EagleServiceSingleEntityQueryRequest request = (EagleServiceSingleEntityQueryRequest)deleteRequestField.get(deleteRequestBuilder);
        List<String> deleteIds = (List<String>)idsField.get(deleteRequestBuilder);
        Assert.assertEquals("query=null&startRowkey=rowkey&pageSize=2147483647&startTime=2016-11-17%2007:54:56&endTime=2016-11-18%2007:54:56&treeAgg=false&metricName=metric&filterIfMissing=true", request.getQueryParameterString());
        Assert.assertEquals("[id1, id2, id3]", deleteIds.toString());
    }

    @Test
    public void testDeleteRequestByQuery() throws Exception {
        EagleServiceConnector connector = mock(EagleServiceConnector.class);
        IEagleServiceClient client = new EagleServiceClientImpl(connector);
        DeleteRequestBuilder deleteRequestBuilder = client.delete().byQuery("AuditService");
        Field deleteRequestField = DeleteRequestBuilder.class.getDeclaredField("request");
        deleteRequestField.setAccessible(true);
        EagleServiceSingleEntityQueryRequest request = (EagleServiceSingleEntityQueryRequest)deleteRequestField.get(deleteRequestBuilder);
        Assert.assertEquals("query=AuditService&pageSize=0&treeAgg=false", request.getQueryParameterString());
    }
}
