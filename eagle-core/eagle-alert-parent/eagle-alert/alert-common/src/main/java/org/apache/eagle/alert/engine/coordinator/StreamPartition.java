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
package org.apache.eagle.alert.engine.coordinator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import java.io.Serializable;
import java.util.*;

/**
 * StreamPartition defines how a data stream is partitioned and sorted
 * streamId is used for distinguishing different streams which are spawned from the same data source
 * type defines how to partition data among slots within one slotqueue
 * columns are fields based on which stream is grouped
 * sortSpec defines how data is sorted.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamPartition implements Serializable {
    private static final long serialVersionUID = -3361648309136926040L;

    private String streamId;
    private Type type;
    private List<String> columns = new ArrayList<>();
    private StreamSortSpec sortSpec;

    public StreamPartition() {
    }

    public StreamPartition(StreamPartition o) {
        this.streamId = o.streamId;
        this.type = o.type;
        this.columns = new ArrayList<String>(o.columns);
        this.sortSpec = o.sortSpec == null ? null : new StreamSortSpec(o.sortSpec);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof StreamPartition)) {
            return false;
        }
        StreamPartition sp = (StreamPartition) other;
        return Objects.equals(streamId, sp.streamId) && Objects.equals(type, sp.type)
            && CollectionUtils.isEqualCollection(columns, sp.columns) && Objects.equals(sortSpec, sp.sortSpec);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(streamId).append(type).append(columns).append(sortSpec).build();
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Type getType() {
        return this.type;
    }

    public enum Type {
        GLOBAL("GLOBAL", 0), GROUPBY("GROUPBY", 1), SHUFFLE("SHUFFLE", 2);
        private final String name;
        private final int index;

        Type(String name, int index) {
            this.name = name;
            this.index = index;
        }

        @Override
        public String toString() {
            return this.name;
        }

        public static Type locate(String type) {
            Type _type = _NAME_TYPE.get(type.toUpperCase());
            if (_type == null) {
                throw new IllegalStateException("Illegal type name: " + type);
            }
            return _type;
        }

        public static Type locate(int index) {
            Type _type = _INDEX_TYPE.get(index);
            if (_type == null) {
                throw new IllegalStateException("Illegal type index: " + index);
            }
            return _type;
        }

        private static final Map<String, Type> _NAME_TYPE = new HashMap<>();
        private static final Map<Integer, Type> _INDEX_TYPE = new TreeMap<>();

        static {
            _NAME_TYPE.put(GLOBAL.name, GLOBAL);
            _NAME_TYPE.put(GROUPBY.name, GROUPBY);
            _NAME_TYPE.put(SHUFFLE.name, SHUFFLE);

            _INDEX_TYPE.put(GLOBAL.index, GLOBAL);
            _INDEX_TYPE.put(GROUPBY.index, GLOBAL);
            _INDEX_TYPE.put(SHUFFLE.index, GLOBAL);
        }
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public StreamSortSpec getSortSpec() {
        return sortSpec;
    }

    public void setSortSpec(StreamSortSpec sortSpec) {
        this.sortSpec = sortSpec;
    }

    @Override
    public String toString() {
        return String.format("StreamPartition[streamId=%s,type=%s,columns=[%s],sortSpec=[%s]]", this.getStreamId(), this.getType(), StringUtils.join(this.getColumns(), ","), sortSpec);
    }
}