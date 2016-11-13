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
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @since Apr 11, 2016.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Publishment implements Serializable {

    private String name;
    private String type;
    private List<String> policyIds;
    private List<String> streamIds;
    private String dedupIntervalMin;
    private List<String> dedupFields;
    private String dedupStateField;
    private String dedupStateCloseValue;
    private OverrideDeduplicatorSpec overrideDeduplicator;
    private Map<String, Object> properties;
    // the class name to extend the IEventSerializer interface
    private String serializer;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDedupStateField() {
        return dedupStateField;
    }

    public void setDedupStateField(String dedupStateField) {
        this.dedupStateField = dedupStateField;
    }

    public String getDedupStateCloseValue() {
        return dedupStateCloseValue;
    }

    public void setDedupStateCloseValue(String dedupStateCloseValue) {
        this.dedupStateCloseValue = dedupStateCloseValue;
    }

    public OverrideDeduplicatorSpec getOverrideDeduplicator() {
        return overrideDeduplicator;
    }

    public void setOverrideDeduplicator(OverrideDeduplicatorSpec overrideDeduplicator) {
        this.overrideDeduplicator = overrideDeduplicator;
    }

    public String getSerializer() {
        return serializer;
    }

    public void setSerializer(String serializer) {
        this.serializer = serializer;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getPolicyIds() {
        return policyIds;
    }

    public void setPolicyIds(List<String> policyIds) {
        this.policyIds = policyIds;
    }

    public List<String> getStreamIds() {
        return streamIds;
    }

    public void setStreamIds(List<String> streamIds) {
        this.streamIds = streamIds;
    }

    public String getDedupIntervalMin() {
        return dedupIntervalMin;
    }

    public void setDedupIntervalMin(String dedupIntervalMin) {
        this.dedupIntervalMin = dedupIntervalMin;
    }

    public List<String> getDedupFields() {
        return dedupFields;
    }

    public void setDedupFields(List<String> dedupFields) {
        this.dedupFields = dedupFields;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Publishment) {
            Publishment p = (Publishment) obj;
            return (Objects.equals(name, p.getName()) && Objects.equals(type, p.getType())
                    && Objects.equals(dedupIntervalMin, p.getDedupIntervalMin())
                    && Objects.equals(dedupFields, p.getDedupFields())
                    && Objects.equals(dedupStateField, p.getDedupStateField())
                    && Objects.equals(overrideDeduplicator, p.getOverrideDeduplicator())
                    && Objects.equals(policyIds, p.getPolicyIds())
                    && Objects.equals(streamIds, p.getStreamIds())
                    && properties.equals(p.getProperties()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(name).append(type).append(dedupIntervalMin).append(dedupFields)
                .append(dedupStateField).append(overrideDeduplicator).append(policyIds).append(streamIds)
                .append(properties).build();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Publishment[name:").append(name).append(",type:").append(type).append(",policyId:")
                .append(policyIds).append(",properties:").append(properties);
        return sb.toString();
    }

}
