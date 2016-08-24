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

package org.apache.eagle.hadoop.queue.storm;

import org.apache.eagle.hadoop.queue.common.HadoopYarnResourceUtils;
import org.apache.eagle.hadoop.queue.common.YarnClusterResourceURLBuilder;
import org.apache.eagle.hadoop.queue.common.YarnURLSelectorImpl;
import org.apache.eagle.hadoop.queue.crawler.ClusterMetricsCrawler;
import org.apache.eagle.hadoop.queue.crawler.RunningAppsCrawler;
import org.apache.eagle.hadoop.queue.crawler.SchedulerInfoCrawler;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.resourcefetch.ha.HAURLSelector;

import backtype.storm.spout.SpoutOutputCollector;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class HadoopQueueRunningExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopQueueRunningExtractor.class);
    private static final int MAX_NUM_THREADS = 10;
    private static final int MAX_WAIT_TIME = 10;
    private static final String DEFAULT_SITE = "sandbox";

    private String site;
    private String urlBases;

    private HAURLSelector urlSelector;
    private ExecutorService executorService;
    private SpoutOutputCollector collector;

    public HadoopQueueRunningExtractor(Config eagleConf, SpoutOutputCollector collector) {
        site = HadoopYarnResourceUtils.getConfigValue(eagleConf, "eagleProps.site", DEFAULT_SITE);
        urlBases = HadoopYarnResourceUtils.getConfigValue(eagleConf, "dataSourceConfig.RMEndPoints", "");
        if (urlBases == null) {
            throw new IllegalArgumentException(site + ".baseurl is null");
        }
        String[] urls = urlBases.split(",");
        urlSelector = new YarnURLSelectorImpl(urls, Constants.CompressionType.GZIP);
        executorService = Executors.newFixedThreadPool(MAX_NUM_THREADS);
        this.collector = collector;
    }

    private void checkUrl() throws IOException {
        if (!urlSelector.checkUrl(YarnClusterResourceURLBuilder.buildRunningAppsURL(urlSelector.getSelectedUrl()))) {
            urlSelector.reSelectUrl();
        }
    }

    public void crawl() {
        try {
            checkUrl();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String selectedUrl = urlSelector.getSelectedUrl();
        LOGGER.info("Current RM base url is " + selectedUrl);
        List<Future<?>> futures = new ArrayList<>();
        futures.add(executorService.submit(new ClusterMetricsCrawler(site, selectedUrl, collector)));
        futures.add(executorService.submit(new RunningAppsCrawler(site, selectedUrl, collector)));
        futures.add(executorService.submit(new SchedulerInfoCrawler(site, selectedUrl, collector)));
        futures.forEach(future -> {
            try {
                future.get(MAX_WAIT_TIME * 1000, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOGGER.info("Caught an overtime exception with message" + e.getMessage());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
    }
}
