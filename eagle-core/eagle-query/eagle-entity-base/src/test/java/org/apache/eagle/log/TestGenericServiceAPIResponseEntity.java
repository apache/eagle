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
package org.apache.eagle.log;

import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import junit.framework.Assert;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * @since 3/18/15
 */
public class TestGenericServiceAPIResponseEntity {
    final static Logger LOG = LoggerFactory.getLogger(TestGenericServiceAPIResponseEntity.class);

    ObjectMapper objectMapper;

    @Before
    public void setUp(){
        objectMapper = new ObjectMapper();
    }

    @JsonSerialize
    public static class Item{
        public Item(){}
        public Item(String name,Double value){
            this.name = name;
            this.value = value;
        }
        private String name;
        private Double value;

        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public Double getValue() {
            return value;
        }
        public void setValue(Double value) {
            this.value = value;
        }
    }

    @Test
    public void testSerDeserialize() throws IOException {
        // mock up service side to serialize
        GenericServiceAPIResponseEntity<Item> entity = new GenericServiceAPIResponseEntity<Item>(Item.class);
        entity.setObj(Arrays.asList(new Item("a",1.2),new Item("b",1.3),new Item("c",1.4)));
        entity.setMeta(new HashMap<String, Object>(){{
            put("tag1","val1");
            put("tag2","val2");
        }});

//        entity.setTypeByObj();
        entity.setSuccess(true);
        String json = objectMapper.writeValueAsString(entity);
        LOG.info(json);

        // mock up client side to deserialize
        GenericServiceAPIResponseEntity deserEntity = objectMapper.readValue(json,GenericServiceAPIResponseEntity.class);
        Assert.assertEquals(json,objectMapper.writeValueAsString(deserEntity));
        Assert.assertEquals(3, deserEntity.getObj().size());
        Assert.assertEquals(LinkedList.class,deserEntity.getObj().getClass());
        Assert.assertEquals(Item.class,deserEntity.getObj().get(0).getClass());
    }
}