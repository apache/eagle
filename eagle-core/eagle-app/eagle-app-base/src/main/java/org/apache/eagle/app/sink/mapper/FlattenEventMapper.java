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
package org.apache.eagle.app.sink.mapper;

import backtype.storm.tuple.Tuple;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class FlattenEventMapper implements StreamEventMapper {

    private final StreamDefinition streamDefinition;
    private final TimestampSelector timestampSelector;

    private final static String DEFAULT_TIMESTAMP_COLUMN_NAME = "timestamp";
    private final static Logger LOG = LoggerFactory.getLogger(FlattenEventMapper.class);

    public FlattenEventMapper(StreamDefinition streamDefinition, TimestampSelector timestampSelector){
        this.streamDefinition = streamDefinition;
        this.timestampSelector = timestampSelector;
    }

    public FlattenEventMapper(StreamDefinition streamDefinition, String timestampFieldName){
        this.streamDefinition = streamDefinition;
        this.timestampSelector = tuple -> tuple.getLongByField(timestampFieldName);
    }

    public FlattenEventMapper(StreamDefinition streamDefinition){
        this(streamDefinition,DEFAULT_TIMESTAMP_COLUMN_NAME);
    }

    @Override
    public List<StreamEvent> map(Tuple tuple) throws Exception {
        Long timestamp = 0L;
        try {
            timestamp = timestampSelector.apply(tuple);
        } catch (Throwable fieldNotExistException){
            if(streamDefinition.isTimeseries()) {
                LOG.error("Stream (streamId = {}) is time series, but failed to detect timestamp, treating as {}", streamDefinition.getStreamId(), timestamp, fieldNotExistException);
            } else{
                /// Ignored for non-timeseries stream
            }
        }

        StreamEvent streamEvent = new StreamEvent(streamDefinition.getStreamId(),
                timestamp,
                this.streamDefinition.getColumns().stream().map((column) -> {
                    Object value = null;
                    try {
                        value = tuple.getValueByField(column.getName());
                    }catch (IllegalArgumentException fieldNotExistException){
                        if(LOG.isDebugEnabled()) {
                            LOG.debug("Column '{}' of stream {} not exist in {}, treating as null", column.getName(), streamDefinition.getStreamId(), tuple, fieldNotExistException);
                        }
                    }
                    return value;
                }).toArray());
        return Collections.singletonList(streamEvent);
    }
}