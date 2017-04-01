/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.topology.storm;

import backtype.storm.spout.SpoutOutputCollector;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.extractor.TopologyCrawler;
import org.apache.eagle.topology.extractor.TopologyExtractorFactory;
import org.apache.eagle.topology.resolver.TopologyRackResolver;
import org.apache.eagle.topology.resolver.impl.ClusterNodeAPITopologyRackResolver;
import org.apache.eagle.topology.resolver.impl.DefaultTopologyRackResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

import static org.apache.eagle.topology.TopologyConstants.*;

public class TopologyDataExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyDataExtractor.class);
    private static final int MIN_WAIT_TIME_SECS = 60;
    private static final double FETCH_TIMEOUT_FACTOR = 0.8;

    private TopologyCheckAppConfig config;
    private List<TopologyCrawler> extractors;
    private ExecutorService executorService;

    public TopologyDataExtractor(TopologyCheckAppConfig topologyCheckAppConfig, SpoutOutputCollector collector) {
        this.config = topologyCheckAppConfig;
        extractors = getExtractors(collector);
        executorService = Executors.newFixedThreadPool(topologyCheckAppConfig.dataExtractorConfig.parseThreadPoolSize);
    }

    public void crawl() {
        List<Future<?>> futures = new ArrayList<>();
        for (TopologyCrawler topologyExtractor : extractors) {
            futures.add(executorService.submit(new DataFetchRunnableWrapper(topologyExtractor)));
        }
        long fetchTimeoutSecs = (long) Math.max(config.dataExtractorConfig.fetchDataIntervalInSecs * FETCH_TIMEOUT_FACTOR, MIN_WAIT_TIME_SECS);
        futures.forEach(future -> {
            try {
                future.get(fetchTimeoutSecs, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOGGER.error("Caught an overtime exception with message" + e.getMessage(), e);
            }
        });
    }

    private List<TopologyCrawler> getExtractors(SpoutOutputCollector collector) {
        List<TopologyCrawler> extractors = new ArrayList<>();
        TopologyRackResolver rackResolver = new DefaultTopologyRackResolver();
        if (config.dataExtractorConfig.resolverCls != null) {
            try {
                rackResolver = config.dataExtractorConfig.resolverCls.newInstance();
                rackResolver.prepare(config);
            } catch (InstantiationException | IllegalAccessException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        for (TopologyType type : config.topologyTypes) {
            try {
                extractors.add(TopologyExtractorFactory.create(type, config, rackResolver, collector));
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        return extractors;
    }

    private static class DataFetchRunnableWrapper implements Runnable {

        private TopologyCrawler topologyExtractor;

        public DataFetchRunnableWrapper(TopologyCrawler topologyExtractor) {
            this.topologyExtractor = topologyExtractor;
        }

        @Override
        public void run() {
            topologyExtractor.extract();
        }
    }


}
