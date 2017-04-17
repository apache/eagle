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
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * This metadata controls how tuple is transformed to stream for
 * example raw data consists of {"metric" : "cpuUsage", "host" :
 * "xyz.com", "timestamp" : 1346846400, "value" : "0.9"} field "metric"
 * is used for creating stream name, here "cpuUsage" is stream name
 *
 * <p>metric could be "cpuUsage", "diskUsage", "memUsage" etc, so
 * activeStreamNames are subset of all metric names</p>
 *
 * <p>All other messages which are not one of activeStreamNames will be
 * filtered out.</p>
 *
 * @since 4/25/16
 */
public class Tuple2StreamMetadata implements Serializable {
    private static final long serialVersionUID = -7297382393695816423L;
    /**
     * only messages belonging to activeStreamNames will be kept while
     * transforming tuple into stream.
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