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

import org.apache.eagle.alert.engine.coordinator.AlertSeverity;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * streamId stands for alert type instead of source event streamId.
 */
public class AlertStreamEvent extends StreamEvent {
    private static final long serialVersionUID = 2392131134670106397L;

    private String alertId;
    private String policyId;
    private StreamDefinition schema;
    private String createdBy;
    private long createdTime;
    private String category;
    private AlertSeverity severity = AlertSeverity.WARNING;

    // ----------------------
    // Lazy Alert Fields
    // ----------------------

    // Dynamical context like app related fields
    private Map<String, Object> context;
    // Alert content like subject and body
    private String subject;
    private String body;

    public AlertStreamEvent() {
    }

    public AlertStreamEvent(AlertStreamEvent event) {
        this.alertId = event.getAlertId();
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

        return String.format("Alert {stream=%S,timestamp=%s,data=%s, policyId=%s, createdBy=%s, metaVersion=%s}",
                this.getStreamId(), DateTimeUtil.millisecondsToHumanDateWithMilliseconds(this.getTimestamp()),
                this.getDataMap(), this.getPolicyId(), this.getCreatedBy(), this.getMetaVersion());
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

    public Map<String, Object> getDataMap() {
        Map<String, Object> event = new HashMap<>();
        for (StreamColumn column : schema.getColumns()) {
            Object obj = this.getData()[schema.getColumnIndex(column.getName())];
            if (obj == null) {
                event.put(column.getName(), null);
                continue;
            }
            event.put(column.getName(), obj);
        }
        return event;
    }

    public Map<String, Object> getContext() {
        return context;
    }

    public void setContext(Map<String, Object> context) {
        this.context = context;
    }

    public String getAlertId() {
        return alertId;
    }

    public void ensureAlertId() {
        if (this.alertId == null) {
            this.alertId = UUID.randomUUID().toString();
        }
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public AlertSeverity getSeverity() {
        return severity;
    }

    public void setSeverity(AlertSeverity severity) {
        this.severity = severity;
    }
}