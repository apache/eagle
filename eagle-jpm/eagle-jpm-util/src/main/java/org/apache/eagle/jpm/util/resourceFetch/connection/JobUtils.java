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
package org.apache.eagle.jpm.util.resourceFetch.connection;

import org.apache.eagle.jpm.util.Constants;

public class JobUtils {
	
	public static String checkAndAddLastSlash(String urlBase) {
		if (!urlBase.endsWith("/")) {
			return urlBase + "/";
		}
		return urlBase;
	}
	
	public static String getJobIDByAppID(String appID) {
		if (appID.startsWith(Constants.APPLICATION_PREFIX)) {
			return appID.replace(Constants.APPLICATION_PREFIX, Constants.JOB_PREFIX);
		}
		return null;
	}

	public static String getAppIDByJobID(String jobID) {
		if (jobID.startsWith(Constants.JOB_PREFIX)) {
			return jobID.replace(Constants.JOB_PREFIX, Constants.APPLICATION_PREFIX);
		}
		return null;
	}
}
