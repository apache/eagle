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
package org.apache.eagle.app.environment.builder;


import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MetricDefinition {

    private NameFormat nameFormat;

    private String nameField = "metric";

    /**
     * Timestamp field name.
     */
    private String timestampField = "timestamp";

    /**
     * Metric dimension field name.
     */
    private List<String> dimensionFields;

    /**
     * Metric value field name.
     */
    private String valueField = "value";

    public NameFormat getNameFormat() {
        return nameFormat;
    }

    public void setNameFormat(NameFormat nameFormat) {
        this.nameFormat = nameFormat;
    }

    public String getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(String timestampField) {
        this.timestampField = timestampField;
    }

    public String getValueField() {
        return valueField;
    }

    public void setValueField(String valueField) {
        this.valueField = valueField;
    }

    public String getNameField() {
        return nameField;
    }

    public void setNameField(String nameField) {
        this.nameField = nameField;
    }

    public List<String> getDimensionFields() {
        return dimensionFields;
    }

    public void setDimensionFields(List<String> dimensionFields) {
        this.dimensionFields = dimensionFields;
    }


    @FunctionalInterface
    public interface NameFormat {
        String getMetricName(Map event);
    }

    public static MetricDefinition namedBy(NameFormat nameFormat) {
        MetricDefinition metricDefinition = new MetricDefinition();
        metricDefinition.nameFormat = nameFormat;
        return metricDefinition;
    }

    public static MetricDefinition namedByField(String nameField) {
        throw new IllegalStateException("TODO: Not implemented yet");
    }

    public MetricDefinition timestampField(String timestampField) {
        this.setTimestampField(timestampField);
        return this;
    }

    public MetricDefinition dimensionFields(String ... dimensionFields) {
        this.setDimensionFields(Arrays.asList(dimensionFields));
        return this;
    }

    public MetricDefinition valueField(String valueField) {
        this.setValueField(valueField);
        return this;
    }
}
