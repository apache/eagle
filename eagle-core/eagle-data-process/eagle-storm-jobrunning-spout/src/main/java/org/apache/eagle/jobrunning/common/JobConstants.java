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
package org.apache.eagle.jobrunning.common;

public class JobConstants {
	public static final String APPLICATION_PREFIX = "application";
	public static final String JOB_PREFIX = "job";
	public static final String V2_APPS_RUNNING_URL = "ws/v1/cluster/apps?state=RUNNING";
	public static final String V2_APPS_COMPLETED_URL = "ws/v1/cluster/apps?state=FINISHED";	
	public static final String V2_APPS_URL = "ws/v1/cluster/apps";
	public static final String ANONYMOUS_PARAMETER = "anonymous=true";
	
	public static final String V2_PROXY_PREFIX_URL = "proxy/";
	public static final String V2_APP_DETAIL_URL = "/ws/v1/mapreduce/jobs";
	public static final String V2_MR_APPMASTER_PREFIX = "/ws/v1/mapreduce/jobs/";
	public static final String V2_CONF_URL = "/conf";
	public static final String V2_COMPLETE_APPS_URL = "ws/v1/cluster/apps/";
	public static final String V2_MR_COUNTERS_URL = "/counters";
	

	public static final String HIVE_QUERY_STRING = "hive.query.string";
	public static final String JOB_STATE_RUNNING = "RUNNING";
	
	public enum YarnApplicationType {
		MAPREDUCE, UNKNOWN
	}
	
	public enum CompressionType {
		GZIP, NONE
	}
	
	public enum ResourceType {
		JOB_RUNNING_INFO, JOB_COMPLETE_INFO, JOB_CONFIGURATION, JOB_LIST
	}
	
	public enum JobState {
		RUNNING, COMPLETED, ALL
	}
}
