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
package org.apache.eagle.jobrunning.callback;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.dataproc.core.EagleOutputCollector;
import org.apache.eagle.dataproc.core.ValuesArray;
import org.apache.eagle.jobrunning.common.JobConstants.ResourceType;
import org.apache.eagle.jobrunning.crawler.JobContext;

public class DefaultRunningJobInputStreamCallback implements RunningJobCallback{
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(DefaultRunningJobInputStreamCallback.class);
	
	private EagleOutputCollector eagleCollector;
	
	public DefaultRunningJobInputStreamCallback(EagleOutputCollector eagleCollector){
		this.eagleCollector = eagleCollector;
	}

	@Override
	public void onJobRunningInformation(JobContext context, ResourceType type, List<Object> objects) {
		String jobId = context.jobId;
		LOG.info(jobId + " information fetched , type: " + type);
		if (type.equals(ResourceType.JOB_CONFIGURATION)) {
			@SuppressWarnings("unchecked")
			Map<String, String> config = (Map<String, String>) objects.get(0);
			// the fist field is fixed as messageId
			RunningJobMessageId messageId = new RunningJobMessageId(jobId, type, context.fetchedTime);
			eagleCollector.collect(new ValuesArray(messageId, context.user, jobId, type, config));
		}
		else if (type.equals(ResourceType.JOB_RUNNING_INFO) || type.equals(ResourceType.JOB_COMPLETE_INFO)) {
			// Here timestamp is meaningless, set to null
			RunningJobMessageId messageId = new RunningJobMessageId(jobId, type, null);
			eagleCollector.collect(new ValuesArray(messageId, context.user, jobId, type, objects));
		}
	}
}
