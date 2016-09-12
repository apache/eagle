/*
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
package org.apache.eagle.alert.metric;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JvmAttributeGaugeSet;
import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.metric.sink.KafkaSink;
import org.apache.eagle.alert.metric.source.JVMMetricSource;
import org.apache.eagle.alert.metric.source.MetricSource;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MetricSystemTest {
    @Test
    @Ignore
    public void testKafkaSink() {
        KafkaSink sink = new KafkaSink();
        MetricRegistry registry = new MetricRegistry();
        registry.registerAll(new JvmAttributeGaugeSet());
        sink.prepare(ConfigFactory.load().getConfig("metric.sink.kafka"), registry);
        sink.report();
        sink.stop();
    }

    @Test
    @Ignore
    public void testMetricSystem() throws InterruptedException {
        MetricSystem system = MetricSystem.load(ConfigFactory.load());
        system.register(new JVMMetricSource());
        system.register(new SampleMetricSource());
        system.start();
        system.report();
        system.stop();
    }

    @Test
    @Ignore
    public void testMetaConflict() {
        MetricSystem system = MetricSystem.load(ConfigFactory.load());
        system.register(new MetaConflictMetricSource());
        system.start();
        system.report();
        system.stop();
    }

    private class MetaConflictMetricSource implements MetricSource {
        private MetricRegistry registry = new MetricRegistry();

        public MetaConflictMetricSource() {
            registry.register("meta.conflict", (Gauge<String>) () -> "meta conflict happening!");
        }

        @Override
        public String name() {
            return "metaConflict";
        }

        @Override
        public MetricRegistry registry() {
            return registry;
        }
    }


    private class SampleMetricSource implements MetricSource {
        private MetricRegistry registry = new MetricRegistry();

        public SampleMetricSource() {
            registry.register("sample.long", (Gauge<Long>) System::currentTimeMillis);
            registry.register("sample.map", (Gauge<Map<String, Object>>) () -> new HashMap<String, Object>() {
                private static final long serialVersionUID = 3948508906655117683L;

                {
                    put("int", 1234);
                    put("str", "text");
                    put("bool", true);
                }
            });
        }

        @Override
        public String name() {
            return "sampleSource";
        }

        @Override
        public MetricRegistry registry() {
            return registry;
        }
    }
}