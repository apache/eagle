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
package org.apache.eagle.alert.metric.entity;

import org.apache.eagle.common.DateTimeUtil;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import java.util.Map;
import java.util.TreeMap;

public class MetricEvent extends TreeMap<String, Object> {

    private static final long serialVersionUID = 6862373651636342744L;

    public static Builder of(String name) {
        return new Builder(name);
    }

    /**
     * TODO: Refactor according to ConsoleReporter.
     */
    public static class Builder {
        private final String name;
        private MetricEvent instance;

        public Builder(String name) {
            this.instance = new MetricEvent();
            this.name = name;
        }

        public MetricEvent build() {
            this.instance.put("name", name);
            if (!this.instance.containsKey("timestamp")) {
                this.instance.put("timestamp", DateTimeUtil.getCurrentTimestamp());
            }
            return this.instance;
        }

        public Builder from(Counter value) {
            // this.instance.put("type","counter");
            this.instance.put("count", value.getCount());
            return this;
        }

        @SuppressWarnings( {"rawtypes", "unchecked"})
        public Builder from(Gauge gauge) {
            Object value = gauge.getValue();
            if (value instanceof Map) {
                Map<? extends String, ?> map = (Map<? extends String, ?>) value;
                this.instance.putAll(map);
            } else {
                this.instance.put("value", value);
            }
            return this;
        }

        public Builder from(Histogram value) {
            this.instance.put("count", value.getCount());
            Snapshot snapshot = value.getSnapshot();
            this.instance.put("min", snapshot.getMin());
            this.instance.put("max", snapshot.getMax());
            this.instance.put("mean", snapshot.getMean());
            this.instance.put("stddev", snapshot.getStdDev());
            this.instance.put("median", snapshot.getMedian());
            this.instance.put("75thPercentile", snapshot.get75thPercentile());
            this.instance.put("95thPercentile", snapshot.get95thPercentile());
            this.instance.put("98thPercentile", snapshot.get98thPercentile());
            this.instance.put("99thPercentile", snapshot.get99thPercentile());
            this.instance.put("999thPercentile", snapshot.get999thPercentile());
            return this;
        }

        public Builder from(Meter value) {
            this.instance.put("value", value.getCount());
            this.instance.put("15MinRate", value.getFifteenMinuteRate());
            this.instance.put("5MinRate", value.getFiveMinuteRate());
            this.instance.put("mean", value.getMeanRate());
            this.instance.put("1MinRate", value.getOneMinuteRate());
            return this;
        }

        public Builder from(Timer value) {
            // this.instance.put("type","timer");
            this.instance.put("value", value.getCount());
            this.instance.put("15MinRate", value.getFifteenMinuteRate());
            this.instance.put("5MinRate", value.getFiveMinuteRate());
            this.instance.put("mean", value.getMeanRate());
            this.instance.put("1MinRate", value.getOneMinuteRate());
            return this;
        }
    }
}