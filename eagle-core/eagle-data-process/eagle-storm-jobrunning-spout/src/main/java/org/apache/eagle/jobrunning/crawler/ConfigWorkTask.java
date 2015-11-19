/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.jobrunning.crawler;

import java.util.List;

import org.apache.eagle.jobrunning.util.JobUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.jobrunning.callback.RunningJobCallback;
import org.apache.eagle.jobrunning.common.JobConstants.ResourceType;

public class ConfigWorkTask implements Runnable {
	
	public JobContext context;
	public ResourceFetcher fetcher;
	public RunningJobCallback callback;
	public RunningJobCrawler crawler;	
	
	private static final Logger LOG = LoggerFactory.getLogger(ConfigWorkTask.class);

	public ConfigWorkTask(JobContext context, ResourceFetcher fetcher, RunningJobCallback callback, RunningJobCrawler crawler) {
		this.context = context;
		this.fetcher = fetcher;
		this.callback = callback;
		this.crawler = crawler;
	}
	
	public void run() {
		runConfigCrawlerWorkhread(context);
	}

	private void runConfigCrawlerWorkhread(JobContext context) {
		LOG.info("Going to fetch job configuration information, jobId:" + context.jobId);
		try {
			List<Object> objs = fetcher.getResource(ResourceType.JOB_CONFIGURATION, JobUtils.getAppIDByJobID(context.jobId));
			callback.onJobRunningInformation(context, ResourceType.JOB_CONFIGURATION, objs);
		}
		catch (Exception ex) {
	        if (ex.getMessage().contains("Server returned HTTP response code: 500")) {
	        	LOG.warn("The server returns 500 error, it's probably caused by job ACL setting, going to skip this job");
	        	// the job remains in processing list, thus we will not do infructuous retry next round
	        	// TODO need remove it from processing list when job finished to avoid memory leak       
	        }
	        else {
	        	LOG.warn("Got an exception when fetching job config: ", ex);
	        	crawler.removeFromProcessingList(ResourceType.JOB_CONFIGURATION, context);
	        }
		}
	}
}
