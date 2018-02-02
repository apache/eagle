/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metadata.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;


import java.util.List;

@Table("eagle_metric")
@ColumnFamily("f")
@Prefix("eagle_metric_schema")
@Service(MetricSchemaEntity.METRIC_SCHEMA_SERVICE)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({"site","metricName","metricGroup"})
public class MetricSchemaEntity extends TaggedLogAPIEntity {
    static final String METRIC_SCHEMA_SERVICE = "MetricSchemaService";
    public static final String METRIC_SITE_TAG = "site";
    public static final String METRIC_NAME_TAG = "metricName";
    public static final String METRIC_GROUP_TAG = "metricGroup";

    @Column("a")
    private List<String> dimensionFields;
    @Column("b")
    private List<String> metricFields;
    @Column("c")
    private String granularity;
    @Column("d")
    private Long modifiedTimestamp;

    public List<String> getDimensionFields() {
        return dimensionFields;
    }

    public void setDimensionFields(List<String> dimensionFields) {
        this.dimensionFields = dimensionFields;
        this.valueChanged("dimensionFields");
    }

    public List<String> getMetricFields() {
        return metricFields;
    }

    public void setMetricFields(List<String> metricFields) {
        this.metricFields = metricFields;
        this.valueChanged("metricFields");
    }

    public String getGranularity() {
        return granularity;
    }

    public void setGranularity(String granularity) {
        this.granularity = granularity;
        this.valueChanged("granularity");
    }

    public void setGranularityByField(int granularity) {
        setGranularity(DateTimeUtil.getCalendarFieldName(granularity));
    }

    public Long getModifiedTimestamp() {
        return modifiedTimestamp;
    }

    public void setModifiedTimestamp(Long modifiedTimestamp) {
        this.modifiedTimestamp = modifiedTimestamp;
        this.valueChanged("modifiedTimestamp");
    }
}