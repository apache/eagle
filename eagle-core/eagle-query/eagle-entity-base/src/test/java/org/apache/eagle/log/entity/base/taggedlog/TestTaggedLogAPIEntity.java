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
package org.apache.eagle.log.entity.base.taggedlog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.eagle.log.base.taggedlog.EntityJsonModule;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.Column;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestTaggedLogAPIEntity {
    private static ObjectMapper objectMapper;

    @BeforeClass
    public static void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.setFilters(TaggedLogAPIEntity.getFilterProvider());
        objectMapper.registerModule(new EntityJsonModule());
    }

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    private class MockSubTaggedLogAPIEntity extends TaggedLogAPIEntity {
        public double getField1() {
            return field1;
        }

        public void setField1(double value) {
            this.field1 = value;
            pcs.firePropertyChange("field1", null, null);
        }

        @Column("a")
        private double field1;

        public String getField2() {
            return field2;
        }

        public void setField2(String field2) {
            this.field2 = field2;
            pcs.firePropertyChange("field2", null, null);
        }

        @Column("b")
        private String field2;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testJsonSerializeFilter() throws IOException {
        MockSubTaggedLogAPIEntity mock = new MockSubTaggedLogAPIEntity();
        Assert.assertTrue(mock instanceof TaggedLogAPIEntity);

        long timestamp = System.currentTimeMillis();
        mock.setTimestamp(timestamp);
        mock.setEncodedRowkey("test_encoded_row_key");
        mock.setPrefix("mock");
        mock.setField2("ok");
        String json = objectMapper.writeValueAsString(mock);
        System.out.println(json);
        Assert.assertTrue(json.contains("field2"));
        Assert.assertTrue(!json.contains("field1"));
        mock.setTimestamp(timestamp);
        mock.setEncodedRowkey("test_encoded_row_key");
        mock.setPrefix("mock");
        mock.setField2("ok");
        mock.setField1(12.345);
        mock.setTags(new HashMap<String, String>() {{
            put("tagName", "tagValue");
        }});
        mock.setExp(new HashMap<String, Object>() {{
            put("extra_field", 3.14);
        }});
        json = objectMapper.writeValueAsString(mock);
        System.out.println(json);
        Assert.assertTrue(json.contains("field2"));
        Assert.assertTrue(json.contains("field1"));
    }

    @Test
    public void testJsonSerializeMap() throws JsonProcessingException {
        Map<List<String>, List<Object>> entries = new HashMap<List<String>, List<Object>>() {
            {
                put(Arrays.asList("a", "b"), Arrays.asList(1, 2, 3));
            }
        };
        String json = objectMapper.writeValueAsString(entries.entrySet());
        Assert.assertNotNull(json);
        System.out.print(json);
    }
}