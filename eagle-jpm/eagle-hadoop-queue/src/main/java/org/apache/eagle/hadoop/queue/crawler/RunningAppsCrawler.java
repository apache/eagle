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
import org.apache.eagle.hadoop.queue.model.applications.AppsWrapper;

import backtype.storm.spout.SpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RunningAppsCrawler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RunningAppsCrawler.class);

    private RunningAppParseListener listener;
    private String urlString;

    public RunningAppsCrawler(String site, String baseUrl, SpoutOutputCollector collector) {
        this.urlString = YarnClusterResourceURLBuilder.buildAcceptedAndRunningAppsURL(baseUrl);
        //this.urlString = YarnClusterResourceURLBuilder.buildRunningAppsURL(baseUrl);
        //this.urlString = YarnClusterResourceURLBuilder.buildFinishedAppsURL(baseUrl);
        listener = new RunningAppParseListener(site, collector, baseUrl);
    }

    @Override
    public void run() {
        try {
            logger.info("Start to crawl app metrics from " + this.urlString);
            AppsWrapper appsWrapper = (AppsWrapper) HadoopYarnResourceUtils.getObjectFromStreamWithGzip(urlString, AppsWrapper.class);
            if (appsWrapper == null || appsWrapper.getApps() == null) {
                logger.error("Failed to crawl running applications with api = " + urlString);
            } else {
                long currentTimestamp = System.currentTimeMillis();
                listener.onMetric(appsWrapper.getApps(), currentTimestamp);
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