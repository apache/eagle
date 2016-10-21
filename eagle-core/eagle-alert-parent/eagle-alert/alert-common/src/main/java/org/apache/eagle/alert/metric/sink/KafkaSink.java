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

package org.apache.eagle.alert.metric.sink;

import org.apache.eagle.alert.metric.MetricConfigs;
import org.apache.eagle.alert.metric.reporter.KafkaReporter;
import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

public class KafkaSink implements MetricSink {
    private KafkaReporter reporter;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);

    @Override
    public void prepare(Config config, MetricRegistry registry) {
        LOG.debug("Preparing kafka-sink");
        KafkaReporter.Builder builder = KafkaReporter.forRegistry(registry)
            .topic(config.getString("topic"))
            .config(config);

        if (config.hasPath(MetricConfigs.TAGS_FIELD_NAME)) {
            builder.addFields(config.getConfig(MetricConfigs.TAGS_FIELD_NAME).root().unwrapped());
        }

        reporter = builder.build();
        LOG.info("Prepared kafka-sink");
    }

    @Override
    public void start(long period, TimeUnit unit) {
        LOG.info("Starting");
        reporter.start(period, unit);
    }

    @Override
    public void stop() {
        LOG.info("Stopping");
        reporter.stop();

        LOG.info("Closing");
        reporter.close();
    }

    @Override
    public void report() {
        reporter.report();
    }
}