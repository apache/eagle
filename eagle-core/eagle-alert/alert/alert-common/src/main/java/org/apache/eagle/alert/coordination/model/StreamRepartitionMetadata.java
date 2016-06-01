package org.apache.eagle.alert.coordination.model;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * @since Apr 25, 2016
 * This meta-data controls how tuple streamId is repartitioned
 */
public class StreamRepartitionMetadata {
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
