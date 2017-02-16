/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.utils;

import org.apache.storm.tuple.Tuple;
import org.apache.eagle.common.utils.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class StreamConvertHelper {
    private static Map tupleToMap(Tuple tuple) {
        Map values = new HashMap<>();
        for (String field : tuple.getFields()) {
            values.put(field, tuple.getValueByField(field));
        }
        return values;
    }

    public static Tuple2<Object,Map> tupleToEvent(Tuple input) {
        Map event = null;
        Object key = input.getValue(0);
        if (input.size() < 2) {
            event = StreamConvertHelper.tupleToMap(input);
        } else {
            Object value = input.getValue(1);
            if (value != null) {
                if (value instanceof Map) {
                    event = (Map) input.getValue(1);
                } else {
                    event = StreamConvertHelper.tupleToMap(input);
                }
            }
        }
        return new Tuple2<>(key, event);
    }
}