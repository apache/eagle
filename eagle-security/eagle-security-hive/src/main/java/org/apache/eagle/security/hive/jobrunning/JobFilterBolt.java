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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.eagle.jobrunning.common.JobConstants;
import org.apache.eagle.jobrunning.common.JobConstants.ResourceType;
import org.apache.eagle.jobrunning.storm.JobRunningContentFilter;
import org.apache.eagle.jobrunning.storm.JobRunningContentFilterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class JobFilterBolt extends BaseRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(JobFilterBolt.class);
    private OutputCollector collector;
	private JobRunningContentFilter filter;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
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
    public void execute(Tuple input) {
		String user = input.getString(0);
        String jobId = input.getString(1);
        ResourceType type = (ResourceType)input.getValue(2);
        if (type.equals(ResourceType.JOB_CONFIGURATION)) {
            Map<String, String> configs = (Map<String, String>)input.getValue(3);
            if (filter.acceptJobConf(configs)) {
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Got a hive job, jobID: " + jobId + ", query: " + configs.get(JobConstants.HIVE_QUERY_STRING));
                } else {
                    LOG.info("Got a hive job, jobID: " + jobId);
                }

                Map<String, Object> map = convertMap(configs);
                collector.emit(Arrays.asList(user, map));
            }
            else {
                LOG.info("skip non hive job, jobId: " + jobId);
            }
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user", "message"));
    }
}
