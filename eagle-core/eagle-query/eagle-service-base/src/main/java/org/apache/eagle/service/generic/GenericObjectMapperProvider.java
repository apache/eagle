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
package org.apache.eagle.service.generic;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ser.FilterProvider;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class GenericObjectMapperProvider implements ContextResolver<ObjectMapper> {
    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    @Override
    public ObjectMapper getContext(Class<?> clazz) {
        return OBJECT_MAPPER;
    }
    public static void setFilter(FilterProvider filter){
        OBJECT_MAPPER.setFilters(filter);
    }

    static{
        setFilter(TaggedLogAPIEntity.getFilterProvider());
        // set more filter here
    }
}
