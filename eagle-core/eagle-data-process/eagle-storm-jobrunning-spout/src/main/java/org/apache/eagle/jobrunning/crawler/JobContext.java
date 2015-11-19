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

public class JobContext {
	public String jobId;
	public String user;
	public Long fetchedTime;
	
	public JobContext() {
		
	}
	
	public JobContext(JobContext context) {
		this.jobId = new String(context.jobId);
		this.user = new String(context.user);
		this.fetchedTime = new Long(context.fetchedTime);
	}
	
	public JobContext(String jobId, String user, Long fetchedTime) {
		this.jobId = jobId;
		this.user = user;
		this.fetchedTime = fetchedTime;
	}
	
	@Override
	public int hashCode() {	
		return jobId.hashCode() ;
	}			
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof JobContext) {
			JobContext context = (JobContext)obj;
			if (this.jobId.equals(context.jobId)) {
				return true;
			}
		}
		return false;
	}
}
