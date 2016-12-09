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


import java.io.Serializable;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class MetricDefinition implements Serializable {

    /**
     * Support simple and complex name format, by default using "metric" field.
     */
    private NameSelector nameSelector = new FieldNameSelector("metric");

    /**
     * Support event/system time, by default using system time.
     */
    private TimestampSelector timestampSelector = new SystemTimestampSelector();

    /**
     * Metric dimension field name.
     */
    private List<String> dimensionFields;

    /**
     * Metric granularity.
     */
    private int granularity = Calendar.MINUTE;

    private String metricType = "DEFAULT";

    /**
     * Metric value field name.
     */
    private String valueField = "value";

    public NameSelector getNameSelector() {
        return nameSelector;
    }

    public void setNameSelector(NameSelector nameSelector) {
        this.nameSelector = nameSelector;
    }

    public String getValueField() {
        return valueField;
    }

    public void setValueField(String valueField) {
        this.valueField = valueField;
    }

    public List<String> getDimensionFields() {
        return dimensionFields;
    }

    public void setDimensionFields(List<String> dimensionFields) {
        this.dimensionFields = dimensionFields;
    }

    public TimestampSelector getTimestampSelector() {
        return timestampSelector;
    }

    public void setTimestampSelector(TimestampSelector timestampSelector) {
        this.timestampSelector = timestampSelector;
    }

    public int getGranularity() {
        return granularity;
    }

    public void setGranularity(int granularity) {
        this.granularity = granularity;
    }

    public String getMetricType() {
        return metricType;
    }

    public void setMetricType(String metricType) {
        this.metricType = metricType;
    }


    @FunctionalInterface
    public interface NameSelector extends Serializable {
        String getMetricName(Map event);
    }

    @FunctionalInterface
    public interface TimestampSelector extends Serializable {
        Long getTimestamp(Map event);
    }

    public MetricDefinition namedBy(NameSelector nameSelector) {
        this.setNameSelector(nameSelector);
        return this;
    }

    /**
     * @see java.util.Calendar
     */
    public MetricDefinition granularity(int granularity) {
        this.setGranularity(granularity);
        return this;
    }

    public MetricDefinition namedByField(String nameField) {
        this.setNameSelector(new FieldNameSelector(nameField));
        return this;
    }

    public static MetricDefinition metricType(String metricType) {
        MetricDefinition metricDefinition = new MetricDefinition();
        metricDefinition.setMetricType(metricType);
        return metricDefinition;
    }

    public MetricDefinition eventTimeByField(String timestampField) {
        this.setTimestampSelector(new EventTimestampSelector(timestampField));
        return this;
    }

    public MetricDefinition dimensionFields(String... dimensionFields) {
        this.setDimensionFields(Arrays.asList(dimensionFields));
        return this;
    }

    public MetricDefinition valueField(String valueField) {
        this.setValueField(valueField);
        return this;
    }

    public class EventTimestampSelector implements TimestampSelector {
        private final String timestampField;

        EventTimestampSelector(String timestampField) {
            this.timestampField = timestampField;
        }

        @Override
        public Long getTimestamp(Map event) {
            if (event.containsKey(timestampField)) {
                Object timestampValue = event.get(timestampField);
                if (timestampValue instanceof Integer) {
                    return Long.valueOf((Integer) timestampValue);
                }
                if (timestampValue instanceof String) {
                    return Long.valueOf((String) timestampValue);
                } else {
                    return (Long) timestampValue;
                }
            } else {
                throw new IllegalArgumentException("Timestamp field '" + timestampField + "' not exists");
            }
        }
    }

    public static class SystemTimestampSelector implements TimestampSelector {
        @Override
        public Long getTimestamp(Map event) {
            return System.currentTimeMillis();
        }
    }

    public static class FieldNameSelector implements NameSelector {
        private final String fieldName;

        FieldNameSelector(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public String getMetricName(Map event) {
            if (event.containsKey(fieldName)) {
                return (String) event.get(fieldName);
            } else {
                throw new IllegalArgumentException("Metric name field '" + fieldName + "' not exists: " + event);
            }
        }
    }
}