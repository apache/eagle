/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.serialization;

import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.serialization.impl.*;

import java.util.HashMap;
import java.util.Map;

public class Serializers {
    private final static Map<StreamColumn.Type,Serializer<?>> COLUMN_TYPE_SER_MAPPING = new HashMap<>();

    public static <T> void register(StreamColumn.Type type,Serializer<T> serializer){
        if(COLUMN_TYPE_SER_MAPPING.containsKey(type)){
            throw new IllegalArgumentException("Duplicated column type: "+type);
        }
        COLUMN_TYPE_SER_MAPPING.put(type,serializer);
    }

    public static <T> Serializer<T> getColumnSerializer(StreamColumn.Type type){
        if(COLUMN_TYPE_SER_MAPPING.containsKey(type)){
            return (Serializer<T>) COLUMN_TYPE_SER_MAPPING.get(type);
        }else{
            throw new IllegalArgumentException("Serializer of type: "+type+" not found");
        }
    }

    public static PartitionedEventSerializer newPartitionedEventSerializer(SerializationMetadataProvider metadataProvider){
        return new PartitionedEventSerializerImpl(metadataProvider);
    }

    static {
        register(StreamColumn.Type.STRING,new StringSerializer());
        register(StreamColumn.Type.INT,new IntegerSerializer());
        register(StreamColumn.Type.LONG,new LongSerializer());
        register(StreamColumn.Type.FLOAT,new FloatSerializer());
        register(StreamColumn.Type.DOUBLE,new DoubleSerializer());
        register(StreamColumn.Type.BOOL,new BooleanSerializer());
        register(StreamColumn.Type.OBJECT,new JavaObjectSerializer());
    }
}