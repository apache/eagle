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

public class MetricDescriptor implements Serializable {

    /**
     * Support simple and complex name format, by default using "metric" field.
     */
    private MetricNameSelector metricNameSelector = new FieldMetricNameSelector("metric");
    private MetricGroupSelector metricGroupSelector = new FixedMetricGroupSelector(DEFAULT_METRIC_GROUP_NAME);
    private SiteIdSelector siteIdSelector = new FieldSiteIdSelector("site");

    private static final String DEFAULT_METRIC_GROUP_NAME = "Default";

    public MetricNameSelector getMetricNameSelector() {
        return metricNameSelector;
    }

    public void setMetricNameSelector(MetricNameSelector metricNameSelector) {
        this.metricNameSelector = metricNameSelector;
    }

    public MetricGroupSelector getMetricGroupSelector() {
        return metricGroupSelector;
    }

    public void setMetricGroupSelector(MetricGroupSelector metricGroupSelector) {
        this.metricGroupSelector = metricGroupSelector;
    }

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

    private String valueField = "value";
    private String resourceField = "resource";

    public String getResourceField() {
        return resourceField;
    }

    public void setResourceField(String resourceField) {
        this.resourceField = resourceField;
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

    public SiteIdSelector getSiteIdSelector() {
        return siteIdSelector;
    }

    public void setSiteIdSelector(SiteIdSelector siteIdSelector) {
        this.siteIdSelector = siteIdSelector;
    }


    @FunctionalInterface
    public interface MetricNameSelector extends Serializable {
        String getMetricName(Map event);
    }

    @FunctionalInterface
    public interface MetricGroupSelector extends Serializable {
        String getMetricGroup(Map event);
    }

    public static class FixedMetricGroupSelector implements MetricGroupSelector {
        private final String groupName;

        private FixedMetricGroupSelector(String groupName) {
            this.groupName = groupName;
        }

        @Override
        public String getMetricGroup(Map event) {
            return groupName;
        }
    }

    @FunctionalInterface
    public interface TimestampSelector extends Serializable {
        Long getTimestamp(Map event);
    }

    @FunctionalInterface
    public interface SiteIdSelector extends Serializable {
        String getSiteId(Map event);
    }

    public class FixedSiteIdSelector implements SiteIdSelector {
        private final String siteId;

        private FixedSiteIdSelector(String siteId) {
            this.siteId = siteId;
        }

        @Override
        public String getSiteId(Map event) {
            return this.siteId;
        }
    }

    private class FieldSiteIdSelector implements SiteIdSelector {
        private final String siteIdFieldName;

        public FieldSiteIdSelector(String siteIdFieldName) {
            this.siteIdFieldName = siteIdFieldName;
        }

        @Override
        public String getSiteId(Map event) {
            return (String) event.getOrDefault(this.siteIdFieldName, "UNKNOWN");
        }
    }

    public MetricDescriptor namedBy(MetricNameSelector metricNameSelector) {
        this.setMetricNameSelector(metricNameSelector);
        return this;
    }

    public MetricDescriptor siteAs(SiteIdSelector siteIdSelector) {
        this.setSiteIdSelector(siteIdSelector);
        return this;
    }

    public MetricDescriptor siteAs(String siteId) {
        this.setSiteIdSelector(new FixedSiteIdSelector(siteId));
        return this;
    }

    public MetricDescriptor siteByField(String fieldName) {
        this.setMetricNameSelector(new FieldMetricNameSelector(fieldName));
        return this;
    }

    /**
     * @see java.util.Calendar
     */
    public MetricDescriptor granularity(int granularity) {
        this.setGranularity(granularity);
        return this;
    }

    public MetricDescriptor namedByField(String nameField) {
        this.setMetricNameSelector(new FieldMetricNameSelector(nameField));
        return this;
    }

    public static MetricDescriptor metricGroupAs(String metricGroupName) {
        return metricGroupAs(new FixedMetricGroupSelector(metricGroupName));
    }

    public static MetricDescriptor metricGroupAs(MetricGroupSelector groupSelector) {
        MetricDescriptor metricDescriptor = new MetricDescriptor();
        metricDescriptor.setMetricGroupSelector(groupSelector);
        return metricDescriptor;
    }

    public static MetricDescriptor metricGroupByField(String fieldName, String defaultGroupName) {
        MetricDescriptor metricDescriptor = new MetricDescriptor();
        metricDescriptor.setMetricGroupSelector((MetricGroupSelector) event -> {
            if (event.containsKey(fieldName)) {
                return (String) event.get(fieldName);
            } else {
                return defaultGroupName;
            }
        });
        return metricDescriptor;
    }

    public static MetricDescriptor metricGroupByField(String fieldName) {
        return metricGroupByField(fieldName, DEFAULT_METRIC_GROUP_NAME);
    }

    public MetricDescriptor eventTimeByField(String timestampField) {
        this.setTimestampSelector(new EventTimestampSelector(timestampField));
        return this;
    }

    public MetricDescriptor dimensionFields(String... dimensionFields) {
        this.setDimensionFields(Arrays.asList(dimensionFields));
        return this;
    }

    public MetricDescriptor valueField(String valueField) {
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

    public static class FieldMetricNameSelector implements MetricNameSelector {
        private final String fieldName;

        FieldMetricNameSelector(String fieldName) {
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