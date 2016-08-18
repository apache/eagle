/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.utils;

import java.io.InputStream;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Since 5/6/16.
 */
public class MetadataSerDeser {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataSerDeser.class);

    @SuppressWarnings("rawtypes")
    public static <K> K deserialize(InputStream is, TypeReference typeRef){
        ObjectMapper mapper = new ObjectMapper();
        try {
            K spec = mapper.readValue(is, typeRef);
            return spec;
        }catch(Exception ex){
            LOG.error("error in deserializing metadata of type {} from input stream",
                    new TypeReference<K>(){}.getType().getClass().getCanonicalName(), ex);
        }
        return null;
    }

    public static <K> K deserialize(InputStream is, Class<K> cls){
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS,true);
        try {
            K spec = mapper.readValue(is, cls);
            return spec;
        }catch(Exception ex){
            LOG.error("Got error to deserialize metadata of type {} from input stream",
                    new TypeReference<K>(){}.getType().getClass().getCanonicalName(), ex);
        }
        return null;
    }

    @SuppressWarnings("rawtypes")
    public static <K> K deserialize(String json, TypeReference typeRef){
        ObjectMapper mapper = new ObjectMapper();
        try {
            K spec = mapper.readValue(json, typeRef);
            return spec;
        }catch(Exception ex){
            LOG.error("error in deserializing metadata of type {} from {}",
                    new TypeReference<K>(){}.getType().getClass().getCanonicalName(), json, ex);
        }
        return null;
    }

    public static <K> K deserialize(String json, Class<K> cls){
        ObjectMapper mapper = new ObjectMapper();
        try {
            K spec = mapper.readValue(json, cls);
            return spec;
        }catch(Exception ex){
            LOG.error("error in deserializing metadata of type {} from {}",
                    new TypeReference<K>(){}.getType().getClass().getCanonicalName(), json, ex);
        }
        return null;
    }

    public static <K> String serialize(K spec){
        ObjectMapper mapper = new ObjectMapper();
        try{
            String json = mapper.writeValueAsString(spec);
            return json;
        }catch(Exception ex){
            LOG.error("error in serializing object {} with type {}", spec,
                    new TypeReference<K>(){}.getType().getClass().getCanonicalName(), ex);
        }
        return null;
    }
}
