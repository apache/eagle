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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.metrics.ElasticsearchReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.Config;

public class ElasticSearchSink implements MetricSink {
    private ElasticsearchReporter reporter = null;
    private final static Logger LOG = LoggerFactory.getLogger(ElasticSearchSink.class);

    @Override
    public void prepare(Config config, MetricRegistry registry) {
        LOG.debug("Preparing elasticsearch-sink");
        try {
            ElasticsearchReporter.Builder builder = ElasticsearchReporter.forRegistry(registry);
            if(config.hasPath("hosts")){
                List<String> hosts = config.getStringList("hosts");
                builder.hosts(hosts.toArray(new String[hosts.size()]));
            }
            if(config.hasPath("index")){
                builder.index(config.getString("index"));
            }
            builder.indexDateFormat("yyyy-MM-dd");
            builder.timestampFieldname(config.hasPath("timestampField")?config.getString("timestampField"):"@timestamp");

            if(config.hasPath("tags")) {
                builder.additionalFields(config.getConfig("tags").root().unwrapped());
            }

            reporter = builder.build();
        } catch (IOException e) {
            LOG.error(e.getMessage(),e);
        }
    }

    @Override
    public void start(long period, TimeUnit unit) {
        reporter.start(period, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        reporter.stop();
        reporter.close();
    }

    @Override
    public void report() {
        reporter.report();
    }
}
