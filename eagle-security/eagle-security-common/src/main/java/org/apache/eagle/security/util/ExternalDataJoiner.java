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
package org.apache.eagle.security.util;

import java.util.HashMap;
import java.util.Map;

import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

/**
 * spawn Quartz scheduler to poll data from eagle service and invoke custom job to handle
 */
public class ExternalDataJoiner {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalDataJoiner.class);
	
	private Scheduler sched;
	private JobDataMap jobDataMap;
	private Class<? extends Job> jobCls;
	private final static String SCHEDULER_NAME = "OuterDataJoiner.scheduler";
	
	private static final String DATA_JOIN_POLL_INTERVALSEC = "dataJoinPollIntervalSec";
	private static final String QUARTZ_GROUP_NAME = "dataJoiner";
	
	private final int defaultIntervalSeconds = 60;
	
	/**
	 * constructor with parameters which are passed from customer
	 * throws Exception while this scheduler can't be brought up
	 * Some protocols:
	 * 1. prop should include an entry keyed by "data.join.poll.intervalInSeconds" if you want to customize poll interval. 
	 * @param prop
	 */
	public ExternalDataJoiner(Class<? extends Job> jobCls, Map<String, Object> prop) throws Exception{
		init(jobCls, prop);
	}
	
	public ExternalDataJoiner(Class<? extends Job> jobCls, Config config) throws Exception{
		Map<String, Object> map = new HashMap<String, Object>();
        for(Map.Entry<String, ConfigValue> entry : config.getObject("eagleProps").entrySet()){
            map.put(entry.getKey(), entry.getValue().unwrapped());
        }

		init(jobCls, map);
	}
	
	private void init(Class<? extends Job> jobCls, Map<String, Object> prop) throws Exception{
		this.jobCls = jobCls;
		jobDataMap = new JobDataMap(prop);
		sched = JVMSingleQuartzScheduler.getInstance().getScheduler();
	}
	
	public void start(){
		// for job
		JobDetail job = JobBuilder.newJob(jobCls) 
		     .withIdentity(jobCls.getName() + ".job", QUARTZ_GROUP_NAME)
		     .setJobData(jobDataMap)
		     .build();
		
		// for trigger
		Object interval = jobDataMap.get(DATA_JOIN_POLL_INTERVALSEC);
		Trigger trigger = TriggerBuilder.newTrigger() 
			  .withIdentity(jobCls.getName() + ".trigger", QUARTZ_GROUP_NAME) 
		      .startNow() 
		      .withSchedule(SimpleScheduleBuilder.simpleSchedule() 
		          .withIntervalInSeconds(interval == null ? defaultIntervalSeconds : ((Integer)interval).intValue()) 
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
