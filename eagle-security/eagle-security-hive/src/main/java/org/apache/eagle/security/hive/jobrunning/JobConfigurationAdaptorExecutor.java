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
package org.apache.eagle.security.hive.jobrunning;

import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.datastream.Tuple2;
import org.apache.eagle.jobrunning.common.JobConstants;
import org.apache.eagle.jobrunning.common.JobConstants.ResourceType;
import org.apache.eagle.jobrunning.storm.JobRunningContentFilter;
import org.apache.eagle.jobrunning.storm.JobRunningContentFilterImpl;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class JobConfigurationAdaptorExecutor extends JavaStormStreamExecutor2<String, Map> {
	private static final long serialVersionUID = 1L;	
	private static final Logger LOG = LoggerFactory.getLogger(JobConfigurationAdaptorExecutor.class);
	private JobRunningContentFilter filter;
	
	@Override
	public void prepareConfig(Config config) {
	}

	@Override
	public void init() {
		filter = new JobRunningContentFilterImpl();
	}
	
	private Map<String, Object> convertMap(Map<String, String> configs) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (Entry<String, String> config : configs.entrySet()) {
			map.put(config.getKey(), config.getValue());
		}
		return map;
	}

    @Override
    public void flatMap(java.util.List<Object> input, Collector<Tuple2<String, Map>> outputCollector){
		String user = (String)input.get(0);
        String jobId = (String)input.get(1);
        ResourceType type = (ResourceType)input.get(2);
        if (type.equals(ResourceType.JOB_CONFIGURATION)) {
            Map<String, String> configs = (Map<String, String>)input.get(3);
            if (filter.acceptJobConf(configs)) {
                LOG.info("Got a hive job, jobID: " + jobId + ", query: " + configs.get(JobConstants.HIVE_QUERY_STRING));
                Map<String, Object> map = convertMap(configs);
                outputCollector.collect(new Tuple2(user, map));
            }
            else {
                LOG.info("skip non hive job, jobId: " + jobId);
            }
        }
    }
}