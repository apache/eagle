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
package org.apache.eagle.dataproc.impl.storm.kafka;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class JsonSerializer implements Serializer<Object> {
    private final StringSerializer stringSerializer = new StringSerializer();
    private static final Logger logger = LoggerFactory.getLogger(JsonSerializer.class);
    private static final ObjectMapper om = new ObjectMapper();

    static {
        om.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, true);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        stringSerializer.configure(configs,isKey);
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        String str = null;
        try {
            str = om.writeValueAsString(data);
        } catch (IOException e) {
            logger.error("Kafka serialization for send error!", e);
        }
        return stringSerializer.serialize(topic, str);
    }

    @Override
    public void close() {
        stringSerializer.close();
    }
}
