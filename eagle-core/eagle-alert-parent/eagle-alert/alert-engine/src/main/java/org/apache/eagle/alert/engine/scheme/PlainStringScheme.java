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

package org.apache.eagle.alert.engine.scheme;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * used for parsing plain string
 */
public class PlainStringScheme implements Scheme {
    private static final long serialVersionUID = 5969724968671646310L;
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(PlainStringScheme.class);
    private String topic;

    @SuppressWarnings("rawtypes")
    public PlainStringScheme(String topic, Map conf){
        this.topic = topic;
    }

    private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;
    public static final String STRING_SCHEME_KEY = "str";

    public static String deserializeString(byte[] buff) {
        return new String(buff, UTF8_CHARSET);
    }

    public Fields getOutputFields() {
        return new Fields(STRING_SCHEME_KEY);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<Object> deserialize(byte[] ser) {
        Map m = new HashMap<>();
        m.put("value", deserializeString(ser));
        m.put("timestamp", System.currentTimeMillis());
        return new Values(topic, m);
    }
}
