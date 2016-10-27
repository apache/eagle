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
package org.apache.eagle.alert.metric.sink;

import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.Config;
import org.apache.eagle.alert.metric.MetricConfigs;
import org.elasticsearch.metrics.ElasticsearchReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ElasticSearchSink implements MetricSink {

    private ElasticsearchReporter reporter = null;
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchSink.class);

    private static final String INDEX_DATEFORMAT_CONF = "indexDateFormat";
    private static final String TIMESTAMP_FIELD_CONF = "timestampField";
    private static final String HOSTS_CONF = "hosts";
    private static final String INDEX_CONF = "index";

    private static final String DEFAULT_INDEX_DATE_FORMAT = "yyyy-MM-dd";
    private static final String DEFAULT_TIMESTAMP_FIELD = "@timestamp";

    @Override
    public void prepare(Config config, MetricRegistry registry) {
        LOG.info("Preparing elasticsearch-sink");
        try {
            ElasticsearchReporter.Builder builder = ElasticsearchReporter.forRegistry(registry);

            if (config.hasPath(HOSTS_CONF)) {
                List<String> hosts = config.getStringList(HOSTS_CONF);
                builder.hosts(hosts.toArray(new String[hosts.size()]));
            }

            if (config.hasPath(INDEX_CONF)) {
                builder.index(config.getString(INDEX_CONF));
            }

            builder.indexDateFormat(config.hasPath(INDEX_DATEFORMAT_CONF)
                ? config.getString(INDEX_DATEFORMAT_CONF) : DEFAULT_INDEX_DATE_FORMAT);

            builder.timestampFieldname(config.hasPath(TIMESTAMP_FIELD_CONF)
                ? config.getString(TIMESTAMP_FIELD_CONF) : DEFAULT_TIMESTAMP_FIELD);

            if (config.hasPath(MetricConfigs.TAGS_FIELD_NAME)) {
                builder.additionalFields(config.getConfig(MetricConfigs.TAGS_FIELD_NAME).root().unwrapped());
            }

            reporter = builder.build();
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new IllegalStateException(e.getMessage(), e);
        }
        LOG.info("Initialized elasticsearch-sink");
    }

    @Override
    public void start(long period, TimeUnit unit) {
        LOG.info("Starting elasticsearch-sink");
        reporter.start(period, unit);
    }

    @Override
    public void stop() {
        LOG.info("Stopping elasticsearch-sink");
        reporter.stop();
        reporter.close();
    }

    @Override
    public void report() {
        reporter.report();
    }
}
