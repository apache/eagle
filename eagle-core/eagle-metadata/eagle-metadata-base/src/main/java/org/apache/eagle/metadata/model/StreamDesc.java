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
package org.apache.eagle.metadata.model;

import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.metadata.utils.StreamIdConversions;

import javax.xml.transform.stream.StreamSource;

public class StreamDesc {
    private String streamId;
    private StreamDefinition schema;
    private StreamSinkConfig sinkConfig;
    private StreamSourceConfig sourceConfig;

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = StreamIdConversions.formStreamTypeId(streamId);
    }

    public StreamDefinition getSchema() {
        return schema;
    }

    public void setSchema(StreamDefinition streamSchema) {
        this.schema = streamSchema;
    }

    public StreamSinkConfig getSinkConfig() {
        return sinkConfig;
    }

    public void setSinkConfig(StreamSinkConfig sinkDesc) {
        this.sinkConfig = sinkDesc;
    }

    public StreamSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public void setSourceConfig(StreamSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }
}