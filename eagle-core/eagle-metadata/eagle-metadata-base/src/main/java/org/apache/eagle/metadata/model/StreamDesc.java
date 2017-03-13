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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StreamDesc)) {
            return false;
        }

        StreamDesc that = (StreamDesc) o;

        if (!getStreamId().equals(that.getStreamId())) {
            return false;
        }
        if (getSchema() != null ? !getSchema().equals(that.getSchema()) : that.getSchema() != null) {
            return false;
        }
        if (getSinkConfig() != null ? !getSinkConfig().equals(that.getSinkConfig()) : that.getSinkConfig() != null) {
            return false;
        }
        return getSourceConfig() != null ? getSourceConfig().equals(that.getSourceConfig()) : that.getSourceConfig() == null;

    }

    @Override
    public int hashCode() {
        int result = getStreamId().hashCode();
        result = 31 * result + (getSchema() != null ? getSchema().hashCode() : 0);
        result = 31 * result + (getSinkConfig() != null ? getSinkConfig().hashCode() : 0);
        result = 31 * result + (getSourceConfig() != null ? getSourceConfig().hashCode() : 0);
        return result;
    }
}