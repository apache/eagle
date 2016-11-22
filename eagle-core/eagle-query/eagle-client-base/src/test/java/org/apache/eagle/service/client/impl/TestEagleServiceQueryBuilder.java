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
import org.apache.commons.lang3.time.DateUtils;
import org.apache.eagle.service.client.EagleServiceQueryBuilder;
import org.apache.eagle.service.client.EagleServiceQueryRequest;
import org.junit.Test;

/**
 * @Since 11/17/16.
 */
public class TestEagleServiceQueryBuilder {

    @Test
    public void testEagleQuery() throws Exception {
        EagleServiceQueryBuilder builder = new EagleServiceQueryBuilder();
        builder.setStartTime(0).setEndTime(DateUtils.MILLIS_PER_DAY).setPageSize(Integer.MAX_VALUE).addReturnField("field1").addReturnField("field2")
            .addReturnTag("tag1").addSearchTag("tagKey", "tagValue");
        EagleServiceQueryRequest request = builder.buildRequest();
        Assert.assertEquals("pageSize=2147483647&startTime=1970-01-01%2000:00:00&endTime=1970-01-02%2000:00:00&tagNameValue=tagKey%3DtagValue&outputTag=tag1&outputField=field1&outputField=field2", request.getQueryParameterString());
    }

}
