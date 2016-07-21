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
package org.apache.eagle.app.sink;

import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.app.ApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class LoggingStreamSink extends StreamSink {
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaStreamSink.class);
    public LoggingStreamSink(StreamDefinition streamDefinition, ApplicationContext applicationContext) {
        super(streamDefinition, applicationContext);
    }

    @Override
    protected void onEvent(StreamEvent streamEvent) {
        LOGGER.info("Receiving {}",streamEvent);
    }

    @Override
    public Map<String, Object> getSinkContext() {
        return new HashMap<>();
    }
}