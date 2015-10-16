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
package eagle.jobrunning.crawler;

import java.util.List;

import eagle.jobrunning.callback.RunningJobCallback;
import eagle.jobrunning.config.RunningJobCrawlConfig;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eagle.job.DefaultJobPartitionerImpl;
import eagle.job.JobFilter;
import eagle.job.JobFilterByPartition;
import eagle.job.JobPartitioner;
import eagle.jobrunning.common.JobConstants.ResourceType;

public class TestRunningJobCrawler {

	private static final Logger LOG = LoggerFactory.getLogger(RunningJobCrawlerImpl.class);
	
	@Test
	public void test() {		
		RunningJobCrawlConfig.RunningJobEndpointConfig endPointConfig = new RunningJobCrawlConfig.RunningJobEndpointConfig();
		endPointConfig.RMBasePaths = new String[1];
		endPointConfig.RMBasePaths[0] = "http://localhost:8088/";
		
		RunningJobCrawlConfig.ControlConfig controlConfig = new RunningJobCrawlConfig.ControlConfig();
		controlConfig.jobConfigEnabled = true;
		controlConfig.partitionerCls = DefaultJobPartitionerImpl.class;
		Class<? extends JobPartitioner> partitionerCls = controlConfig.partitionerCls;
		JobPartitioner partitioner = null;
		try {
			partitioner = partitionerCls.newInstance();
		} catch (Exception e) {
			LOG.error("failing instantiating job partitioner class " + partitionerCls.getCanonicalName());
			throw new IllegalStateException(e);
		}
		
		//TODO: replace null parameter with an instance of ZKStateConfig
		RunningJobCrawlConfig config = new RunningJobCrawlConfig(endPointConfig, controlConfig, null);

		JobFilter jobFilter = new JobFilterByPartition(partitioner, 1, 0);
		RunningJobCallback callback = new RunningJobCallback() {
			private static final long serialVersionUID = 1L;
			@Override
			public void onJobRunningInformation(JobContext jobContext, ResourceType type, List<Object> objects) {
				LOG.info("jobID: " + jobContext + ", type: " + type + " objects: " + objects);
			}
		};
		try {
			//TODO: construct zkStateManager
			RunningJobCrawlerImpl crawler = new RunningJobCrawlerImpl(config, null, callback, jobFilter, null);
			while (true) {			
				crawler.crawl();
				Thread.sleep(5 *1000);
			}
		} catch (Exception e) {
			LOG.error("failing creating crawler driver");
			throw new IllegalStateException(e);
		}
	}
}
