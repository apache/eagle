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

package org.apache.eagle.service.metadata.resource;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.Tuple2StreamMetadata;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.scheme.JsonStringStreamNameSelector;

import java.util.Properties;

public class StreamDefinitionWrapper {
    private Kafka2TupleMetadata streamSource;
    private StreamDefinition streamDefinition;

    public Kafka2TupleMetadata getStreamSource() {
        return streamSource;
    }

    public void setStreamSource(Kafka2TupleMetadata streamSource) {
        this.streamSource = streamSource;
    }

    public StreamDefinition getStreamDefinition() {
        return streamDefinition;
    }

    public void setStreamDefinition(StreamDefinition streamDefinition) {
        this.streamDefinition = streamDefinition;
    }

    public void ensureDefault() {
        String dataSourceName = (getStreamDefinition().getStreamId() + "_SOURCE").toUpperCase();
        getStreamDefinition().setDataSource(dataSourceName);
        getStreamSource().setName(dataSourceName);
        Tuple2StreamMetadata codec = new Tuple2StreamMetadata();
        codec.setTimestampColumn("timestamp");
        codec.setStreamNameSelectorCls(JsonStringStreamNameSelector.class.getName());
        codec.setStreamNameSelectorProp(new Properties(){{
            put("userProvidedStreamName", streamSource.getName());
        }});
        this.streamSource.setCodec(codec);
    }
}