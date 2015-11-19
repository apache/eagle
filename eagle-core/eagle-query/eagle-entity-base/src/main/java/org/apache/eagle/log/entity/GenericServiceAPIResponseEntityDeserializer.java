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
package org.apache.eagle.log.entity;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.ObjectCodec;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;

import java.io.IOException;
import java.util.*;

/**
 * @since 3/18/15
 */
public class GenericServiceAPIResponseEntityDeserializer extends JsonDeserializer<GenericServiceAPIResponseEntity> {
    private final static String META_FIELD="meta";
    private final static String SUCCESS_FIELD="success";
    private final static String EXCEPTION_FIELD="exception";
    private final static String OBJ_FIELD="obj";
    private final static String TYPE_FIELD="type";

    @Override
    public GenericServiceAPIResponseEntity deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        GenericServiceAPIResponseEntity entity = new GenericServiceAPIResponseEntity();
        ObjectCodec objectCodec = jp.getCodec();

        JsonNode rootNode = jp.getCodec().readTree(jp);
        if(rootNode.isObject()){
            Iterator<Map.Entry<String,JsonNode>> fields = rootNode.getFields();
            JsonNode objNode = null;
            while(fields.hasNext()){
                Map.Entry<String,JsonNode> field = fields.next();
                if (META_FIELD.equals(field.getKey()) && field.getValue() != null)
                    entity.setMeta(objectCodec.readValue(field.getValue().traverse(), Map.class));
                else if(SUCCESS_FIELD.equals(field.getKey()) && field.getValue() != null){
                    entity.setSuccess(field.getValue().getValueAsBoolean(false));
                }else if(EXCEPTION_FIELD.equals(field.getKey()) && field.getValue() != null){
                    entity.setException(field.getValue().getTextValue());
                }else if(TYPE_FIELD.endsWith(field.getKey())  && field.getValue() != null){
                    try {
                        entity.setType(Class.forName(field.getValue().getTextValue()));
                    } catch (ClassNotFoundException e) {
                        throw new IOException(e);
                    }
                }else if(OBJ_FIELD.equals(field.getKey()) && field.getValue() != null){
                    objNode = field.getValue();
                }
            }

            if(objNode!=null) {
                JavaType collectionType=null;
                if (entity.getType() != null) {
                    collectionType = TypeFactory.defaultInstance().constructCollectionType(LinkedList.class, entity.getType());
                }else{
                    collectionType = TypeFactory.defaultInstance().constructCollectionType(LinkedList.class, Map.class);
                }
                List obj = objectCodec.readValue(objNode.traverse(), collectionType);
                entity.setObj(obj);
            }
        }else{
            throw new IOException("root node is not object");
        }
        return entity;
    }
}