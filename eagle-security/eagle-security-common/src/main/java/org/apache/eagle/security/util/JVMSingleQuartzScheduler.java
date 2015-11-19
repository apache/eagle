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

import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Quartz scheduler is created not thread safe, so a singleton has to be there if we want a JVM level single scheduler
 */
public class JVMSingleQuartzScheduler {
	private static final Logger LOG = LoggerFactory.getLogger(JVMSingleQuartzScheduler.class);
	
	private static JVMSingleQuartzScheduler instance = new JVMSingleQuartzScheduler();
	private Scheduler sched;
	
	public static JVMSingleQuartzScheduler getInstance(){
		return instance;
	}
	
	private JVMSingleQuartzScheduler(){
		try{
			SchedulerFactory schedFact = new org.quartz.impl.StdSchedulerFactory(); 
			sched = schedFact.getScheduler(); 
			sched.start();
		}catch(Exception ex){
			LOG.error("Fail starting quartz scheduler", ex);
			throw new IllegalStateException(ex);
		}
	}
	
	public Scheduler getScheduler(){
		return sched;
	}
}
