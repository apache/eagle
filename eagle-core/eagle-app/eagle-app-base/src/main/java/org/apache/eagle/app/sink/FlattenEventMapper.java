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
package org.apache.eagle.app.sink;

import org.apache.eagle.alert.engine.model.StreamEvent;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class FlattenEventMapper implements StreamEventMapper {
    private final String streamId;
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final Logger LOGGER = LoggerFactory.getLogger(FlattenEventMapper.class);

    public FlattenEventMapper(String streamId) {
        this.streamId = streamId;
    }

    @Override
    public List<StreamEvent> map(Tuple tuple) throws Exception {
        long timestamp;
        if (tuple.getFields().contains(TIMESTAMP_FIELD)) {
            try {
                timestamp = tuple.getLongByField("timestamp");
            } catch (Exception ex) {
                // if timestamp is not null
                LOGGER.error(ex.getMessage(), ex);
                timestamp = 0;
            }
        } else {
            timestamp = System.currentTimeMillis();
        }
        Object[] values = new Object[tuple.getFields().size()];
        for (int i = 0; i < tuple.getFields().size(); i++) {
            values[i] = tuple.getValue(i);
        }
        StreamEvent event = new StreamEvent();
        event.setTimestamp(timestamp);
        event.setStreamId(streamId);
        event.setData(values);
        return Collections.singletonList(event);
    }
}
