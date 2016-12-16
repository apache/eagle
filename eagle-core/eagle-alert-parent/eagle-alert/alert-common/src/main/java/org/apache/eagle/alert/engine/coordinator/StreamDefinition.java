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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This is actually a data source schema.
 *
 * @since Apr 5, 2016
 */
public class StreamDefinition implements Serializable {
    private static final long serialVersionUID = 2352202882328931825L;

    // Stream unique ID
    private String streamId;

    // Stream description
    private String description;

    // Is validateable or not
    private boolean validate;

    // Is timeseries-based stream or not
    private boolean timeseries;

    // TODO: Decouple dataSource and siteId from stream definition

    // Stream data source ID
    private String dataSource;

    // Tenant (Site) ID
    private String siteId;

    private List<StreamColumn> columns = new ArrayList<>();

    public String toString() {
        return String.format("StreamDefinition[streamId=%s, dataSource=%s, description=%s, validate=%s, timeseries=%s, columns=%s",
            streamId,
            dataSource,
            description,
            validate,
            timeseries,
            columns);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .append(this.streamId)
            .append(this.description)
            .append(this.validate)
            .append(this.timeseries)
            .append(this.dataSource)
            .append(this.siteId)
            .append(this.columns)
            .build();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StreamDefinition)) {
            return false;
        }
        return Objects.equals(this.streamId, ((StreamDefinition) obj).streamId)
            && Objects.equals(this.description, ((StreamDefinition) obj).description)
            && Objects.equals(this.validate, ((StreamDefinition) obj).validate)
            && Objects.equals(this.timeseries, ((StreamDefinition) obj).timeseries)
            && Objects.equals(this.dataSource, ((StreamDefinition) obj).dataSource)
            && Objects.equals(this.siteId, ((StreamDefinition) obj).siteId)
            && CollectionUtils.isEqualCollection(this.columns, ((StreamDefinition) obj).columns);
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isValidate() {
        return validate;
    }

    public void setValidate(boolean validate) {
        this.validate = validate;
    }

    public boolean isTimeseries() {
        return timeseries;
    }

    public void setTimeseries(boolean timeseries) {
        this.timeseries = timeseries;
    }

    @XmlElementWrapper(name = "columns")
    @XmlElement(name = "column")
    public List<StreamColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<StreamColumn> columns) {
        this.columns = columns;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public int getColumnIndex(String column) {
        int i = 0;
        for (StreamColumn col : this.getColumns()) {
            if (col.getName().equals(column)) {
                return i;
            }
            i++;
        }
        return -1;
    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public StreamDefinition copy() {
        StreamDefinition copied = new StreamDefinition();
        copied.setColumns(this.getColumns());
        copied.setDataSource(this.getDataSource());
        copied.setDescription(this.getDescription());
        copied.setSiteId(this.getSiteId());
        copied.setStreamId(this.getStreamId());
        copied.setTimeseries(this.isTimeseries());
        copied.setValidate(this.isValidate());
        return copied;
    }
}