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
import org.apache.eagle.log.entity.RowkeyQueryAPIResponseEntity;
import org.apache.eagle.log.entity.test.TestEntity;
import org.apache.eagle.service.client.RowkeyQueryAPIResponseConvertHelper;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

/**
 * @Since 11/17/16.
 */
public class TestRowkeyQueryAPIResponseConvertHelper {
    @Test
    public void testConvert() throws Exception {
        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put("prefix", "eagle");
        objectMap.put("timestamp", 1479264382L);
        objectMap.put("encodedRowkey", "rowkey");
        Map<String, String> tags = new HashMap<>();
        tags.put("field1", "value1");
        objectMap.put("tags", tags);
        objectMap.put("remediationID", "ID");
        objectMap.put("remediationStatus", "status");
        objectMap.put("count", 123456789L);
        objectMap.put("numHosts", 9);
        objectMap.put("numClusters", 123456789L);
        RowkeyQueryAPIResponseEntity rowkeyQueryAPIResponseEntity = new RowkeyQueryAPIResponseEntity();
        rowkeyQueryAPIResponseEntity.setObj(objectMap);
        RowkeyQueryAPIResponseConvertHelper rowkeyQueryAPIResponseConvertHelper = new RowkeyQueryAPIResponseConvertHelper();
        rowkeyQueryAPIResponseEntity = rowkeyQueryAPIResponseConvertHelper.convert(TestEntity.class, rowkeyQueryAPIResponseEntity);
        TestEntity entity = (TestEntity)rowkeyQueryAPIResponseEntity.getObj();
        Assert.assertEquals("eagle", entity.getPrefix());
        Assert.assertEquals(1479264382L, entity.getTimestamp());
        Assert.assertEquals("rowkey", entity.getEncodedRowkey());
        Assert.assertEquals("ID", entity.getRemediationID());
        Assert.assertEquals("status", entity.getRemediationStatus());
        Assert.assertEquals(1, entity.getTags().size());
    }
}
