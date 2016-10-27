/**
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
package org.apache.eagle.alert.engine.model;

import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.utils.DateTimeUtil;
import org.apache.commons.lang3.StringUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * streamId stands for alert type instead of source event streamId.
 */
public class AlertStreamEvent extends StreamEvent {
    private static final long serialVersionUID = 2392131134670106397L;

    private String policyId;
    private StreamDefinition schema;
    private String createdBy;
    private long createdTime;
    // app related fields
    private Map<String, Object> extraData;

    public AlertStreamEvent() {
    }

    public AlertStreamEvent(AlertStreamEvent event) {
        this.policyId = event.policyId;
        this.schema = event.schema;
        this.createdBy = event.createdBy;
        this.createdTime = event.createdTime;
        this.setTimestamp(event.getTimestamp());
        this.setData(new Object[event.data.length]);
        System.arraycopy(event.data, 0, this.data, 0, event.data.length);
        this.setStreamId(event.getStreamId());
        this.setMetaVersion(event.getMetaVersion());
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public String getPolicyId() {
        return policyId;
    }

    @Override
    public String toString() {
        List<String> dataStrings = new ArrayList<>(this.getData().length);
        for (Object obj : this.getData()) {
            if (obj != null) {
                dataStrings.add(obj.toString());
            } else {
                dataStrings.add(null);
            }
        }
        return String.format("AlertStreamEvent[stream=%S,timestamp=%s,data=[%s], policyId=%s, createdBy=%s, metaVersion=%s]",
            this.getStreamId(), DateTimeUtil.millisecondsToHumanDateWithMilliseconds(this.getTimestamp()),
            StringUtils.join(dataStrings, ","), this.getPolicyId(), this.getCreatedBy(), this.getMetaVersion());
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public StreamDefinition getSchema() {
        return schema;
    }

    public void setSchema(StreamDefinition schema) {
        this.schema = schema;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }

    public Map<String, String> getDataMap() {
        Map<String, String> event = new HashMap<>();
        for (StreamColumn column : schema.getColumns()) {
            Object obj = this.getData()[schema.getColumnIndex(column.getName())];
            if (obj == null) {
                event.put(column.getName(), null);
                continue;
            }
            if (column.getName().equalsIgnoreCase("timestamp") && obj instanceof Long) {
                String eventTime = DateTimeUtil.millisecondsToHumanDateWithSeconds(((Long) obj).longValue());
                event.put(column.getName(), eventTime);
            } else {
                event.put(column.getName(), obj.toString());
            }
        }
        return event;
    }

    public Map<String, Object> getExtraData() {
        return extraData;
    }

    public void setExtraData(Map<String, Object> extraData) {
        this.extraData = extraData;
    }

}
