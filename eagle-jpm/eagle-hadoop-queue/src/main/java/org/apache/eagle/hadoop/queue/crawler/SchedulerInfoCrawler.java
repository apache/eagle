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

import backtype.storm.spout.SpoutOutputCollector;
import org.apache.eagle.hadoop.queue.common.YarnClusterResourceURLBuilder;
import org.apache.eagle.hadoop.queue.model.scheduler.*;
import org.apache.eagle.hadoop.queue.common.HadoopYarnResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SchedulerInfoCrawler implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(SchedulerInfoCrawler.class);

	private SchedulerInfoParseListener listener;
    private String urlString;

	public SchedulerInfoCrawler(String site, String baseUrl, SpoutOutputCollector collector) {
		this.urlString = YarnClusterResourceURLBuilder.buildSchedulerInfoURL(baseUrl);
		this.listener = new SchedulerInfoParseListener(site, collector);
	}

	@Override
	public void run() {
		try {
			//https://some.server.address:50030/ws/v1/cluster/scheduler?anonymous=true
			logger.info("Start to crawl cluster scheduler queues from " + this.urlString);
			SchedulerWrapper schedulerWrapper = (SchedulerWrapper) HadoopYarnResourceUtils.getObjectFromStreamWithGzip(urlString, SchedulerWrapper.class);
			if (schedulerWrapper == null || schedulerWrapper.getScheduler() == null) {
				logger.error("Failed to crawl scheduler info with url = " + this.urlString);
			} else {
				SchedulerInfo scheduler = schedulerWrapper.getScheduler().getSchedulerInfo();
				logger.info("Crawled " + scheduler.getQueues().getQueue().size() + " queues");
				long currentTimestamp = System.currentTimeMillis();
				listener.onMetric(scheduler, currentTimestamp);
			}
		} catch (IOException e) {
			logger.error("Got IO exception while connecting to "+this.urlString + " : "+ e.getMessage());
		} catch (Exception e) {
			logger.error("Got exception while crawling queues:" + e.getMessage(), e);
		} catch (Throwable e) {
			logger.error("Got throwable exception while crawling queues:" + e.getMessage(), e);
		} finally {
			listener.flush();
		}
	}
}