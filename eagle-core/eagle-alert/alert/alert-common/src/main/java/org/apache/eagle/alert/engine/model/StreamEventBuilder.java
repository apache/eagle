package org.apache.eagle.alert.engine.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamEventBuilder{
    private final static Logger LOG = LoggerFactory.getLogger(StreamEventBuilder.class);

    private StreamEvent instance;
    private StreamDefinition streamDefinition;
    public StreamEventBuilder(){
        instance = new StreamEvent();
    }

    public StreamEventBuilder schema(StreamDefinition streamDefinition){
        this.streamDefinition = streamDefinition;
        if(instance.getStreamId() == null) instance.setStreamId(streamDefinition.getStreamId());
        return this;
    }

    public StreamEventBuilder streamId(String streamId){
        instance.setStreamId(streamId);
        return this;
    }

    public StreamEventBuilder attributes(Map<String,Object> data, StreamDefinition streamDefinition){
        this.schema(streamDefinition);
        List<StreamColumn> columnList = streamDefinition.getColumns();
        if(columnList!=null && columnList.size() > 0){
            List<Object> values = new ArrayList<>(columnList.size());
            for (StreamColumn column : columnList) {
                values.add(data.getOrDefault(column.getName(),column.getDefaultValue()));
            }
            instance.setData(values.toArray());
        } else if(LOG.isDebugEnabled()){
            LOG.warn("All data [{}] are ignored as no columns defined in schema {}",data,streamDefinition);
        }
        return this;
    }

    public StreamEventBuilder attributes(Map<String,Object> data){
        return attributes(data,this.streamDefinition);
    }

    public StreamEventBuilder attributes(Object ... data){
        instance.setData(data);
        return this;
    }

    public StreamEventBuilder timestamep(long timestamp){
        instance.setTimestamp(timestamp);
        return this;
    }

    public StreamEvent build(){
        if(instance.getStreamId() == null){
            throw new IllegalArgumentException("streamId is null of event: " + instance);
        }
        return instance;
    }

    public StreamEventBuilder copyFrom(StreamEvent event) {
        this.instance.copyFrom(event);
        return this;
    }
}