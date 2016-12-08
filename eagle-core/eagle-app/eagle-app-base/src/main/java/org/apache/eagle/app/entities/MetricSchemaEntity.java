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
package org.apache.eagle.app.entities;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@Table("eagle_metric")
@ColumnFamily("f")
@Prefix("eagle_metric_schema")
@Service(MetricSchemaEntity.METRIC_SCHEMA_SERVICE)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({"name","group"})
public class MetricSchemaEntity extends TaggedLogAPIEntity {
    static final String METRIC_SCHEMA_SERVICE = "MetricSchemaService";
    public static final String METRIC_NAME = "name";
    public static final String METRIC_GROUP = "group";

    public static final String GENERIC_METRIC_VALUE_NAME = "name";

    @Column("a")
    private List<String> dimensions;
    @Column("b")
    private Map<String,Class<?>> metrics;
    @Column("c")
    private String description;

    public List<String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<String> dimensions) {
        this.dimensions = dimensions;
    }

    public Map<String, Class<?>> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, Class<?>> metrics) {
        this.metrics = metrics;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}