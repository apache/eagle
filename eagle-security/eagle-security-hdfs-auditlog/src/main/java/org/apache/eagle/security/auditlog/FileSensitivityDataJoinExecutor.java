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
package org.apache.eagle.security.auditlog;

import com.typesafe.config.Config;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.datastream.Tuple2;
import org.apache.eagle.security.auditlog.timer.FileSensitivityPollingJob;
import org.apache.eagle.security.auditlog.util.SimplifyPath;
import org.apache.eagle.security.hdfs.entity.FileSensitivityAPIEntity;
import org.apache.eagle.security.util.ExternalDataCache;
import org.apache.eagle.security.util.ExternalDataJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileSensitivityDataJoinExecutor extends JavaStormStreamExecutor2<String, Map> {
	private static final Logger LOG = LoggerFactory.getLogger(FileSensitivityDataJoinExecutor.class);
	private Config config;
	
	@Override
	public void prepareConfig(Config config) {
		this.config = config;		
	}

	@Override
	public void init() {
		// start IPZone data polling
		try{
			ExternalDataJoiner joiner = new ExternalDataJoiner(FileSensitivityPollingJob.class, config);
			joiner.start();
		}catch(Exception ex){
			LOG.error("Fail bring up quartz scheduler", ex);
			throw new IllegalStateException(ex);
		}
	}

    @Override
    public void flatMap(java.util.List<Object> input, Collector<Tuple2<String, Map>> outputCollector){
        Map<String, Object> event = (Map<String, Object>)input.get(1);
        Map<String, FileSensitivityAPIEntity> map = (Map<String, FileSensitivityAPIEntity>) ExternalDataCache.getInstance().getJobResult(FileSensitivityPollingJob.class);
        FileSensitivityAPIEntity e = null;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Receive map: " + map + "event: " + event);
        }

        String src = (String)event.get("src");
        if(map != null && src != null) {
            String simplifiedPath = new SimplifyPath().build(src);
            for (String fileDir : map.keySet()) {
                Pattern pattern = Pattern.compile(simplifiedPath,Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(fileDir);
                boolean isMatched = matcher.matches();
                if (isMatched) {
                    e = map.get(fileDir);
                    break;
                }
            }
        }
        event.put("sensitivityType",  e == null ? "NA" : e.getSensitivityType());
        if(LOG.isDebugEnabled()) {
            LOG.debug("After file sensitivity lookup: " + event);
        }
        // LOG.info(">>>> After file sensitivity lookup: " + event);
        outputCollector.collect(new Tuple2(event.get("user"), event));
    }
}
