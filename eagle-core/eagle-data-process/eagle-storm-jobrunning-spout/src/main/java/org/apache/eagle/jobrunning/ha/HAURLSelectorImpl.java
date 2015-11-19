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
package org.apache.eagle.jobrunning.ha;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.eagle.jobrunning.url.ServiceURLBuilder;
import org.apache.eagle.jobrunning.util.InputStreamUtils;
import org.apache.eagle.jobrunning.common.JobConstants;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HAURLSelectorImpl implements HAURLSelector {

	private final String[] urls;
	private volatile String selectedUrl;
	private final ServiceURLBuilder builder;
	
	private volatile boolean reselectInProgress;
	private final JobConstants.CompressionType compressionType;
	private static final long MAX_RETRY_TIME = 3;
	private static final Logger LOG = LoggerFactory.getLogger(HAURLSelectorImpl.class);
	
	public HAURLSelectorImpl(String[] urls, ServiceURLBuilder builder, JobConstants.CompressionType compressionType) {
		this.urls = urls;
		this.compressionType = compressionType;
		this.builder = builder;
	}
	
	public boolean checkUrl(String urlString) {
		InputStream is = null;
		try {
			is = InputStreamUtils.getInputStream(urlString, compressionType);
		}
		catch (Exception ex) {
			LOG.info("get inputstream from url: " + urlString + " failed. ");
			return false;
		}
		finally {
			if (is != null) { try {	is.close(); } catch (IOException e) {/*Do nothing*/} }
		}
		return true;
	}

	@Override
	public String getSelectedUrl() {
		if (selectedUrl == null) {
			selectedUrl = urls[0];
		}
		return selectedUrl;
	}
	
	@Override
	public void reSelectUrl() throws IOException {
		if (reselectInProgress) return;
		synchronized(this) {
			if (reselectInProgress) return;
			reselectInProgress = true;
			try {
				LOG.info("Going to reselect url");
				for (int i = 0; i < urls.length; i++) {		
					String urlToCheck = urls[i];
					LOG.info("Going to try url :" + urlToCheck);
					for (int time = 0; time < MAX_RETRY_TIME; time++) {
						if (checkUrl(builder.build(urlToCheck, JobConstants.JobState.RUNNING.name()))) {
							selectedUrl = urls[i];
							LOG.info("Successfully switch to new url : " + selectedUrl);
							return;
						}
						LOG.info("try url " + urlToCheck + "fail for " + (time+1) + " times, sleep 5 seconds before try again. ");
						try {
							Thread.sleep(5 * 1000);
						}
						catch (InterruptedException ex) { /* Do Nothing */}
					}
				}
				throw new IOException("No alive url found: "+ StringUtils.join(";", Arrays.asList(this.urls)));
			}
			finally {
				reselectInProgress = false;
			}
		}
	}
}
