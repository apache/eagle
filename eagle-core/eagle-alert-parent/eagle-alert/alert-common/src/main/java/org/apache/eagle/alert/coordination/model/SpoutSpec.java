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

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * SpoutSpec metadata control 3 phases for data transformation for one specific topic
 * phase 1: kafka topic to tuple, controlled by Kafka2TupleMetadata, i.e. Scheme
 * phase 2: tuple to stream, controlled by Tuple2StreamMetadata, i.e. stream name selector etc.
 * phase 3: stream repartition, controlled by StreamRepartitionMetadata, i.e. groupby spec
 * @since Apr 18, 2016
 *
 */
public class SpoutSpec {
    private String version;

//    private String spoutId;
    private String topologyId;

    // topicName -> kafka2TupleMetadata
    private Map<String, Kafka2TupleMetadata> kafka2TupleMetadataMap = new HashMap<String, Kafka2TupleMetadata>();
    // topicName -> Tuple2StreamMetadata
    private Map<String, Tuple2StreamMetadata> tuple2StreamMetadataMap = new HashMap<String, Tuple2StreamMetadata>();
    // topicName -> list of StreamRepartitionMetadata, here it is list because one topic(data source) may spawn multiple streams.
    private Map<String, List<StreamRepartitionMetadata>> streamRepartitionMetadataMap = new HashMap<String, List<StreamRepartitionMetadata>>();

    public SpoutSpec(){}

    public SpoutSpec(
            String topologyId,
//            String spoutId,
            Map<String, List<StreamRepartitionMetadata>>  streamRepartitionMetadataMap,
            Map<String, Tuple2StreamMetadata> tuple2StreamMetadataMap,
            Map<String, Kafka2TupleMetadata>  kafka2TupleMetadataMap) {
        this.topologyId = topologyId;
//        this.spoutId = spoutId;
        this.streamRepartitionMetadataMap = streamRepartitionMetadataMap;
        this.tuple2StreamMetadataMap = tuple2StreamMetadataMap;
        this.kafka2TupleMetadataMap = kafka2TupleMetadataMap;
    }

//    public String getSpoutId() {
//        return spoutId;
//    }
//    public void setSpoutId(String spoutId) {
//        this.spoutId = spoutId;
//    }

    public String getTopologyId() {
        return topologyId;
    }

    public Map<String, List<StreamRepartitionMetadata>> getStreamRepartitionMetadataMap() {
        return streamRepartitionMetadataMap;
    }

    public Map<String, Tuple2StreamMetadata> getTuple2StreamMetadataMap(){
        return this.tuple2StreamMetadataMap;
    }

    public Map<String, Kafka2TupleMetadata> getKafka2TupleMetadataMap() {
        return kafka2TupleMetadataMap;
    }

    @JsonIgnore
    public StreamRepartitionMetadata getStream(String streamName) {
        for (List<StreamRepartitionMetadata> meta : this.streamRepartitionMetadataMap.values()) {
            Optional<StreamRepartitionMetadata> m = meta.stream().filter((t) -> t.getStreamId().equalsIgnoreCase(streamName)).findFirst();
            if (m.isPresent()) {
                return m.get();
            }
        }
        return null;
    }

    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
    }

    public void setKafka2TupleMetadataMap(Map<String, Kafka2TupleMetadata> kafka2TupleMetadataMap) {
        this.kafka2TupleMetadataMap = kafka2TupleMetadataMap;
    }

    public void setTuple2StreamMetadataMap(Map<String, Tuple2StreamMetadata> tuple2StreamMetadataMap) {
        this.tuple2StreamMetadataMap = tuple2StreamMetadataMap;
    }

    public void setStreamRepartitionMetadataMap(Map<String, List<StreamRepartitionMetadata>> streamRepartitionMetadataMap) {
        this.streamRepartitionMetadataMap = streamRepartitionMetadataMap;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return String.format("version:%s-topo:%s ", version, this.topologyId);
    }

}
