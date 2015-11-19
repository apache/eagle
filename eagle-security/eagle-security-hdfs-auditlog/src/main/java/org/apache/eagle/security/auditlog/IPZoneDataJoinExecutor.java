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
import org.apache.eagle.security.auditlog.timer.IPZonePollingJob;
import org.apache.eagle.security.hdfs.entity.IPZoneEntity;
import org.apache.eagle.security.util.ExternalDataCache;
import org.apache.eagle.security.util.ExternalDataJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class IPZoneDataJoinExecutor extends JavaStormStreamExecutor2<String, Map> {
	private static final Logger LOG = LoggerFactory.getLogger(IPZoneDataJoinExecutor.class);
	private Config config;
	
	@Override
	public void prepareConfig(Config config) {
		this.config = config;
	}

	@Override
	public void init() {
		// start IPZone data polling
		try{
			ExternalDataJoiner joiner = new ExternalDataJoiner(IPZonePollingJob.class, config);
			joiner.start();
		}catch(Exception ex){
			LOG.error("Fail bring up quartz scheduler", ex);
			throw new IllegalStateException(ex);
		}
	}

    @Override
    public void flatMap(java.util.List<Object> input, Collector<Tuple2<String, Map>> outputCollector){
        Map<String, Object> event = (Map<String, Object>)input.get(1);
        Map<String, IPZoneEntity> map = (Map<String, IPZoneEntity>) ExternalDataCache.getInstance().getJobResult(IPZonePollingJob.class);
        IPZoneEntity e = null;
        if(map != null){
            e = map.get(event.get("host"));
        }
        event.put("securityZone",  e == null ? "NA" : e.getSecurityZone());
        if(LOG.isDebugEnabled()) LOG.debug("After IP zone lookup: " + event);
        outputCollector.collect(new Tuple2(event.get("user"), event));
    }
}
