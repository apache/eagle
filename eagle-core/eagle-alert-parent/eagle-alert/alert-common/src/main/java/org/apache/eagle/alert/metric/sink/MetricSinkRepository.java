package org.apache.eagle.alert.metric.sink;

import java.util.HashMap;
import java.util.Map;

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
public class MetricSinkRepository {
    private final static Map<String,Class<? extends MetricSink>> sinkTypeClassMapping = new HashMap<>();

    public static void register(String sinkType,Class<? extends MetricSink> sinkClass){
        sinkTypeClassMapping.put(sinkType,sinkClass);
    }

    public static MetricSink createSink(String sinkType){
        if (!sinkTypeClassMapping.containsKey(sinkType)) {
            throw new IllegalArgumentException("Unknown sink type: "+sinkType);
        }
        try {
            return sinkTypeClassMapping.get(sinkType).newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    static {
        register("kafka",KafkaSink.class);
        register("jmx",JmxSink.class);
        register("elasticsearch",ElasticSearchSink.class);
        register("stdout",ConsoleSink.class);
        register("logger",Slf4jSink.class);
    }
}