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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;

public class PublishPartition implements Serializable {

    private static final long serialVersionUID = 2524776632955586234L;

    private String policyId;
    private String streamId;
    private String publishId;
    private Set<String> columns = new HashSet<>();

    @JsonIgnore
    private Set<Object> columnValues = new HashSet<>();

    public PublishPartition() {
    }

    public PublishPartition(String streamId, String policyId, String publishId, Set<String> columns) {
        this.streamId = streamId;
        this.policyId = policyId;
        this.publishId = publishId;
        if (columns != null) {
            this.columns = columns;
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(streamId).append(policyId).append(publishId).append(columns).append(columnValues).build();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof PublishPartition
            && Objects.equal(this.streamId, ((PublishPartition) obj).getStreamId())
            && Objects.equal(this.policyId, ((PublishPartition) obj).getPolicyId())
            && Objects.equal(this.publishId, ((PublishPartition) obj).getPublishId())
            && CollectionUtils.isEqualCollection(this.columns, ((PublishPartition) obj).getColumns())
            && CollectionUtils.isEqualCollection(this.columnValues, ((PublishPartition) obj).getColumnValues());
    }

    @Override
    public String toString() {
        return String.format("PublishPartition[policyId=%s,streamId=%s,publishId=%s,columns=%s,columnValues=%s]",
            policyId, streamId, publishId, columns, columnValues);
    }

    @Override
    public PublishPartition clone() {
        return new PublishPartition(this.streamId, this.policyId, this.publishId, new HashSet<>(this.columns));
    }

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public String getPublishId() {
        return publishId;
    }

    public void setPublishId(String publishId) {
        this.publishId = publishId;
    }

    public Set<String> getColumns() {
        return columns;
    }

    public void setColumns(Set<String> columns) {
        this.columns = columns;
    }

    @JsonIgnore
    public Set<Object> getColumnValues() {
        return columnValues;
    }

    @JsonIgnore
    public void setColumnValues(Set<Object> columnValues) {
        this.columnValues = columnValues;
    }

}
