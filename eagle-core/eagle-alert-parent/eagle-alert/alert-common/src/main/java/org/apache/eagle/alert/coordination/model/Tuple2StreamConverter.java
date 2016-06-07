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
package org.apache.eagle.alert.coordination.model;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert incoming tuple to stream
 * incoming tuple consists of 2 fields, topic and map of key/value
 * output stream consists of 3 fields, stream name, timestamp, and map of key/value
 */
public class Tuple2StreamConverter {
    private static final Logger LOG = LoggerFactory.getLogger(Tuple2StreamConverter.class);
    private Tuple2StreamMetadata metadata;
    private StreamNameSelector cachedSelector;
    public Tuple2StreamConverter(Tuple2StreamMetadata metadata){
        this.metadata = metadata;
        try {
            cachedSelector = (StreamNameSelector)Class.forName(metadata.getStreamNameSelectorCls()).
                    getConstructor(Properties.class).
                    newInstance(metadata.getStreamNameSelectorProp());
        }catch(Exception ex){
            LOG.error("error initializing StreamNameSelector object", ex);
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Assume tuple is composed of topic + map of key/value
     * @param tuple
     * @return
     */
    @SuppressWarnings({ "unchecked" })
    public List<Object> convert(List<Object> tuple){
        Map<String, Object> m = (Map<String, Object>)tuple.get(1);
        String streamName = cachedSelector.getStreamName(m);
        if(!metadata.getActiveStreamNames().contains(streamName)) {
            if(LOG.isDebugEnabled()) {
                LOG.debug("streamName {} is not within activeStreamNames {}", streamName, metadata.getActiveStreamNames());
            }
            return null;
        }

        Object timeObject = m.get(metadata.getTimestampColumn());
        long timestamp = 0L;
        if(timeObject instanceof Number){
            timestamp = ((Number) timeObject).longValue();
        }else{
            String timestampFieldValue = (String) m.get(metadata.getTimestampColumn());
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(metadata.getTimestampFormat());
                timestamp = sdf.parse(timestampFieldValue).getTime();
            } catch (Exception ex) {
                LOG.error("continue with current timestamp because error happens while parsing timestamp column " + metadata.getTimestampColumn() + " with format " + metadata.getTimestampFormat());
                timestamp = System.currentTimeMillis();
            }
        }
        return Arrays.asList(tuple.get(0), streamName, timestamp, tuple.get(1));
    }
}
