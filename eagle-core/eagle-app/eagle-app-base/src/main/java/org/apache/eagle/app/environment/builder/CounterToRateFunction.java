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

import com.google.common.base.Preconditions;
import org.apache.eagle.app.utils.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CounterToRateFunction implements TransformFunction {
    private static final Logger LOG = LoggerFactory.getLogger(CounterToRateFunction.class);
    private final Map<String, CounterValue> cache;
    private MetricDescriptor metricDescriptor;
    private Collector collector;

    public CounterToRateFunction(MetricDescriptor metricDescriptor, long heartbeat, TimeUnit unit, final Clock clock) {
        final long heartbeatMillis = TimeUnit.MILLISECONDS.convert(heartbeat, unit);
        this.cache = new LinkedHashMap<String, CounterValue>(16, 0.75f, true) {
            protected boolean removeEldestEntry(Map.Entry<String, CounterValue> eldest) {
                final long now = clock.now();
                final long lastMod = eldest.getValue().getTimestamp();
                final boolean expired = (now - lastMod) > heartbeatMillis;
                if (expired) {
                    LOG.debug("heartbeat interval exceeded, expiring {}", eldest.getKey());
                }
                return expired;
            }
        };
        this.metricDescriptor = metricDescriptor;
    }

    @Override
    public String getName() {
        return "CounterToRate";
    }

    @Override
    public void open(Collector collector) {
        this.collector = collector;
    }

    @Override
    public void transform(Map event) {
        Metric metric = toMetric(event);
        LOG.debug("received {} metrics", metric);
        if (metric.isCounter()) {
            final String metricName = metric.getMetricName();
            final CounterValue prev = cache.get(metricName);
            if (prev != null) {
                final double rate = prev.computeRate(metric);
                event.put(metricDescriptor.getValueField(), rate);
                collector.collect(event.toString(), event);
            } else {
                CounterValue current = new CounterValue(metric);
                cache.put(metricName, current);
            }
        } else {
            collector.collect(event.toString(), event);
        }

    }

    @Override
    public void close() {
        cache.clear();
    }

    private Metric toMetric(Map event) {

        String metricName = "";
        for (String dimensionField : metricDescriptor.getDimensionFields()) {
            metricName += event.get(dimensionField) + "-";
        }
        metricName += metricDescriptor.getMetricNameSelector().getMetricName(event);

        long timestamp = metricDescriptor.getTimestampSelector().getTimestamp(event);

        return new Metric(metricName, timestamp, getCurrentValue(event));
    }

    private double getCurrentValue(Map event) {
        double[] values;
        if (event.containsKey(metricDescriptor.getValueField())) {
            values = new double[]{(double) event.get(metricDescriptor.getValueField())};
        } else {
            LOG.warn("Event has no value field '{}': {}, use 0 by default", metricDescriptor.getValueField(), event);
            values = new double[]{0};
        }
        return values[0];
    }

    protected static class CounterValue {
        private long timestamp;
        private double value;

        public CounterValue(long timestamp, double value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        public CounterValue(Metric m) {
            this(m.getTimestamp(), m.getNumberValue().doubleValue());
        }

        public long getTimestamp() {
            return timestamp;
        }

        public double computeRate(Metric m) {
            final long currentTimestamp = m.getTimestamp();
            final double currentValue = m.getNumberValue().doubleValue();

            final long durationMillis = currentTimestamp - timestamp;
            final double delta = currentValue - value;

            timestamp = currentTimestamp;
            value = currentValue;

            return computeRate(durationMillis, delta);
        }

        private double computeRate(long durationMillis, double delta) {
            final double millisPerSecond = 1000.0;
            final double duration = durationMillis / millisPerSecond;
            return (duration <= 0.0 || delta <= 0.0) ? 0.0 : delta / duration;
        }

        @Override
        public String toString() {
            return "CounterValue{" + "timestamp=" + timestamp + ", value=" + value + '}';
        }
    }


    protected final class Metric {
        private final String metricName;
        private final long timestamp;
        private final Object value;

        public Metric(String metricName, long timestamp, Object value) {
            this.metricName = Preconditions.checkNotNull(metricName, "metricName");
            this.timestamp = timestamp;
            this.value = Preconditions.checkNotNull(value, "value");
        }

        public String getMetricName() {
            return metricName;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public Object getValue() {
            return value;
        }

        public Number getNumberValue() {
            return (Number) value;
        }

        public boolean hasNumberValue() {
            return (value instanceof Number);
        }

        public boolean isCounter() {
            return metricName.endsWith("count");
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Metric)) {
                return false;
            }
            Metric m = (Metric) obj;
            return metricName.equals(m.getMetricName())
                && timestamp == m.getTimestamp()
                && value.equals(m.getValue());
        }

        @Override
        public int hashCode() {
            int result = metricName.hashCode();
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + value.hashCode();
            return result;
        }


        @Override
        public String toString() {
            return "Metric{metricName=" + metricName + ", timestamp=" + timestamp + ", value=" + value + '}';
        }
    }
}