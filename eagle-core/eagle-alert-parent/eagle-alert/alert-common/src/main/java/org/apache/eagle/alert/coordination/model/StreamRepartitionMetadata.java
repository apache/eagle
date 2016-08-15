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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * @since Apr 25, 2016
 * This meta-data controls how tuple streamId is repartitioned
 */
public class StreamRepartitionMetadata  implements Serializable {
    private String topicName;
    private String streamId;
    /**
     * each stream may have multiple different grouping strategies,for example groupby some fields or even shuffling
     */
    public List<StreamRepartitionStrategy> groupingStrategies = new ArrayList<StreamRepartitionStrategy>();

    public StreamRepartitionMetadata(){}

    public StreamRepartitionMetadata(String topicName, String stream) {
        this.topicName = topicName;
        this.streamId = stream;
    }

    public String getStreamId() {
        return streamId;
    }
    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public String getTopicName() {
        return topicName;
    }
    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public List<StreamRepartitionStrategy> getGroupingStrategies() {
        return groupingStrategies;
    }

    @JsonIgnore
    public void addGroupStrategy(StreamRepartitionStrategy gs) {
        this.groupingStrategies.add(gs);
    }

    public void setGroupingStrategies(List<StreamRepartitionStrategy> groupingStrategies) {
        this.groupingStrategies = groupingStrategies;
    }
}
