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

package org.apache.eagle.hadoop.queue.crawler;

import org.apache.eagle.hadoop.queue.common.HadoopYarnResourceUtils;
import org.apache.eagle.hadoop.queue.common.YarnClusterResourceURLBuilder;
import org.apache.eagle.hadoop.queue.model.cluster.ClusterMetrics;
import org.apache.eagle.hadoop.queue.model.cluster.ClusterMetricsWrapper;
import backtype.storm.spout.SpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ClusterMetricsCrawler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ClusterMetricsCrawler.class);
    private ClusterMetricsParseListener listener;
    private String urlString;

    public ClusterMetricsCrawler(String site, String urlBase, SpoutOutputCollector collector) {
        listener = new ClusterMetricsParseListener(site, collector);
        urlString = YarnClusterResourceURLBuilder.buildClusterMetricsURL(urlBase);
    }

    @Override
    public void run() {
        try {
            logger.info("Start to crawl cluster metrics from " + this.urlString);
            ClusterMetricsWrapper metricsWrapper = (ClusterMetricsWrapper) HadoopYarnResourceUtils.getObjectFromUrlStream(urlString, ClusterMetricsWrapper.class);
            ClusterMetrics metrics = metricsWrapper.getClusterMetrics();
            if (metrics == null) {
                logger.error("Failed to crawl cluster metrics");
            } else {
                long currentTimestamp = System.currentTimeMillis();
                listener.onMetric(metrics, currentTimestamp);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.trace(e.getMessage(), e);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            listener.flush();
        }
    }
}