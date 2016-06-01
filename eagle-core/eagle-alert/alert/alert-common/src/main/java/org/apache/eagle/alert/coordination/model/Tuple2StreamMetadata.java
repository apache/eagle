package org.apache.eagle.alert.coordination.model;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @Since 4/25/16. This metadata controls how tuple is transformed to stream for
 *        example raw data consists of {"metric" : "cpuUsage", "host" :
 *        "xyz.com", "timestamp" : 1346846400, "value" : "0.9"} field "metric"
 *        is used for creating stream name, here "cpuUsage" is stream name
 *
 *        metric could be "cpuUsage", "diskUsage", "memUsage" etc, so
 *        activeStreamNames are subset of all metric names
 *
 *        All other messages which are not one of activeStreamNames will be
 *        filtered out
 */
public class Tuple2StreamMetadata {
    /**
     * only messages belonging to activeStreamNames will be kept while
     * transforming tuple into stream
     */
    private Set<String> activeStreamNames = new HashSet<String>();
    // the specific stream name selector
    private Properties streamNameSelectorProp;
    private String streamNameSelectorCls;
    private String timestampColumn;
    private String timestampFormat;

    public Set<String> getActiveStreamNames() {
        return activeStreamNames;
    }

    public void setActiveStreamNames(Set<String> activeStreamNames) {
        this.activeStreamNames = activeStreamNames;
    }

    public Properties getStreamNameSelectorProp() {
        return streamNameSelectorProp;
    }

    public void setStreamNameSelectorProp(Properties streamNameSelectorProp) {
        this.streamNameSelectorProp = streamNameSelectorProp;
    }

    public String getStreamNameSelectorCls() {
        return streamNameSelectorCls;
    }

    public void setStreamNameSelectorCls(String streamNameSelectorCls) {
        this.streamNameSelectorCls = streamNameSelectorCls;
    }

    public String getTimestampColumn() {
        return timestampColumn;
    }

    public void setTimestampColumn(String timestampColumn) {
        this.timestampColumn = timestampColumn;
    }

    public String getTimestampFormat() {
        return timestampFormat;
    }

    public void setTimestampFormat(String timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

}