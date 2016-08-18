/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.security.enrich;

import com.typesafe.config.Config;
import org.apache.eagle.security.util.JVMSingleQuartzScheduler;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * spawn Quartz scheduler to poll data from eagle service and invoke custom job to handle
 */
public class ExternalDataJoiner {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalDataJoiner.class);

	private Scheduler sched;
	private JobDataMap jobDataMap;
	private String jobId;
	private String taskId;
	private Config config;

	private static final String DATA_JOIN_POLL_INTERVALSEC = "dataJoinPollIntervalSec";
	private static final String QUARTZ_GROUP_NAME = "dataJoiner";

	private final int defaultIntervalSeconds = 60;

	/**
	 * constructor with parameters which are passed from customer
	 * throws Exception while this scheduler can't be brought up
	 * Some protocols:
	 * 1. prop should include an entry keyed by "data.join.poll.intervalInSeconds" if you want to customize poll interval.
	 */
	public ExternalDataJoiner(DataEnrichLCM lcm, Config config, String taskId) throws Exception{
		this.taskId = taskId;
		Map<String, Object> map = new HashMap<>();
		// add DataEnrichLCM to JobDataMap
		map.put("dataEnrichLCM", lcm);
		this.config = config;
		init(lcm.getClass().getCanonicalName(), map);
	}

	private void init(String jobId, Map<String, Object> prop) throws Exception{
		this.jobId = jobId;
		jobDataMap = new JobDataMap(prop);
		sched = JVMSingleQuartzScheduler.getInstance().getScheduler();
	}

	public void start(){
		// for job
		String group = String.format("%s.%s.%s.%s",
				QUARTZ_GROUP_NAME,
				config.hasPath("siteId") ? config.getString("siteId") : "",
				config.hasPath("appId") ? config.getString("appId") : "",
				taskId);
		JobDetail job = JobBuilder.newJob(DataEnrichJob.class)
		     .withIdentity(jobId + ".job", group)
		     .usingJobData(jobDataMap)
		     .build();

		// for trigger
		Object interval = jobDataMap.get(DATA_JOIN_POLL_INTERVALSEC);
        int dataJoinPollIntervalSec = (interval == null ? defaultIntervalSeconds : Integer.parseInt(interval.toString()));
		Trigger trigger = TriggerBuilder.newTrigger()
			  .withIdentity(jobId + ".trigger", group)
		      .startNow()
		      .withSchedule(SimpleScheduleBuilder.simpleSchedule()
		          .withIntervalInSeconds(dataJoinPollIntervalSec)
		          .repeatForever())
		      .build();
		try{
			sched.scheduleJob(job, trigger);
		}catch(Exception ex){
			LOG.error("Can't schedule job " + job.getDescription(), ex);
		}
	}

	public void stop() throws Exception{
		sched.shutdown();
	}
}
