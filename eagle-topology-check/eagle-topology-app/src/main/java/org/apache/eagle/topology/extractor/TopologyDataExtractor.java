/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.topology.extractor;

import backtype.storm.spout.SpoutOutputCollector;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.apache.eagle.topology.TopologyConstants.*;

public class TopologyDataExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyDataExtractor.class);
    private static final int MAX_NUM_THREADS = 10;
    private static final int MAX_WAIT_TIME = 10;
    private static final String DEFAULT_SITE = "sandbox";

    private TopologyCheckAppConfig config;
    private List<TopologyExtractorBase> extractors;
    private SpoutOutputCollector collector;
    private ExecutorService executorService;

    public TopologyDataExtractor(TopologyCheckAppConfig topologyCheckAppConfig, SpoutOutputCollector collector) {
        this.config = topologyCheckAppConfig;
        extractors = getExtractors();
        executorService = Executors.newFixedThreadPool(MAX_NUM_THREADS);
    }

    public void crawl() {
        List<Future<?>> futures = new ArrayList<>();
        for (TopologyExtractorBase topologyExtractor : extractors) {
            futures.add(executorService.submit(new DataFetchRunnableWrapper(topologyExtractor)));
        }
        futures.forEach(future -> {
            try {
                future.get(MAX_WAIT_TIME, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.info("Caught an overtime exception with message" + e.getMessage());
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        });
    }

    private List<TopologyExtractorBase> getExtractors() {
        List<TopologyExtractorBase> extractors = new ArrayList<>();
        for (TopologyType type : config.topologyTypes) {
            extractors.add(TopologyExtractorFactory.create(type, config));
        }
        return extractors;
    }

    private static class DataFetchRunnableWrapper implements Runnable {

        private TopologyExtractorBase topologyExtractor;

        public DataFetchRunnableWrapper(TopologyExtractorBase topologyExtractor) {
            this.topologyExtractor = topologyExtractor;
        }

        @Override
        public void run() {
            topologyExtractor.extract();
        }
    }


}
