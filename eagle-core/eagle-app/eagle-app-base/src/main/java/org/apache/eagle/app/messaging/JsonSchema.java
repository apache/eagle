/*
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
package org.apache.eagle.app.messaging;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * General Json Schema.
 * Different from org.apache.eagle.alert.engine.scheme.JsonScheme which is just to multi-topic cases.
 *
 * @see org.apache.eagle.alert.engine.scheme.JsonScheme
 */
public class JsonSchema implements Scheme {
    private static final long serialVersionUID = -8352896475656975577L;
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(JsonSchema.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Fields getOutputFields() {
        return new Fields("f1","f2");
    }

    @Override
    @SuppressWarnings("rawtypes")
    public List<Object> deserialize(ByteBuffer ser) {
        try {
            if (ser != null) {
                Map map = mapper.readValue(ser.array(), Map.class);
                return Arrays.asList(map.hashCode(), map);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Content is null, ignore");
                }
            }
        } catch (IOException e) {
            try {
                LOG.error("Failed to deserialize as JSON: {}", new String(ser.array(), "UTF-8"), e);
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
            }
        }
        return null;
    }
}