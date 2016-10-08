/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.security.auditlog.kafka;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import storm.kafka.StringScheme;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MessageJsonScheme  implements Scheme {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(MessageJsonScheme.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static final String MESSAGE_SCHEME_KEY = "message";

    @Override
    public Fields getOutputFields() {
        return new Fields(StringScheme.STRING_SCHEME_KEY);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public List<Object> deserialize(byte[] ser) {
        try {
            if (ser != null) {
                Map map = mapper.readValue(ser, Map.class);
                Object message = map.get(MESSAGE_SCHEME_KEY);
                if (message != null) {
                    return new Values(map.get(MESSAGE_SCHEME_KEY));
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Content is null, ignore");
                }
            }
        } catch (IOException e) {
            try {
                LOG.error("Failed to deserialize as JSON: {}", new String(ser, "UTF-8"), e);
            } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
            }
        }
        return null;
    }
}