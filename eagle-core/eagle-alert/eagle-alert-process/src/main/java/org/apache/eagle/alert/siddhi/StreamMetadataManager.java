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
package org.apache.eagle.alert.siddhi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.entity.AlertStreamSchemaEntity;
import com.typesafe.config.Config;
import org.apache.eagle.alert.dao.AlertStreamSchemaDAO;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.commons.collections.map.UnmodifiableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * centralized memory where all stream metadata sit on, it is not mutable data
 */
public class StreamMetadataManager {
	private static final Logger LOG = LoggerFactory.getLogger(StreamMetadataManager.class);
	
	private static StreamMetadataManager instance = new StreamMetadataManager();
	private Map<String, List<AlertStreamSchemaEntity>> map = new HashMap<String, List<AlertStreamSchemaEntity>>();
	private Map<String, SortedMap<String, AlertStreamSchemaEntity>> map2 = new HashMap<String, SortedMap<String, AlertStreamSchemaEntity>>();
	private volatile boolean initialized = false;
	
	private StreamMetadataManager(){
	}
	
	public static StreamMetadataManager getInstance(){
		return instance;
	}
	
	private void internalInit(Config config, AlertStreamSchemaDAO dao){
		try{
			String dataSource = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.DATA_SOURCE);
			List<AlertStreamSchemaEntity> list = dao.findAlertStreamSchemaByDataSource(dataSource);
			if(list == null)
				return;
			for (AlertStreamSchemaEntity entity : list) {
				String streamName = entity.getTags().get(AlertConstants.STREAM_NAME);
				if (map.get(streamName) == null) {
					map.put(streamName, new ArrayList<AlertStreamSchemaEntity>());
					map2.put(streamName, new TreeMap<String, AlertStreamSchemaEntity>());
				}
				map.get(streamName).add(entity);
				map2.get(streamName).put(entity.getTags().get(AlertConstants.ATTR_NAME), entity);
			}
		}catch(Exception ex){
			LOG.error("Fail building metadata manger", ex);
			throw new IllegalStateException(ex);
		}
	}
	
	/**
	 * singleton with init would be good for unit test as well, and it ensures that
	 * initialization happens only once before you use it.  
	 * @param config
	 * @param dao
	 */
	public void init(Config config, AlertStreamSchemaDAO dao){
		if(!initialized){
			synchronized(this){
				if(!initialized){
                    if(LOG.isDebugEnabled()) LOG.debug("Initializing ...");
					internalInit(config, dao);
					initialized = true;
                    LOG.info("Successfully initialized");
				}
			}
		}else{
            LOG.info("Already initialized, skip");
        }
	}

	// Only for unit test purpose
	public void reset() {
		synchronized (this) {
			initialized = false;
			map.clear();
			map2.clear();
		}
	}

	private void ensureInitialized(){
		if(!initialized)
			throw new IllegalStateException("StreamMetadataManager should be initialized before using it");
	}
	
	public List<AlertStreamSchemaEntity> getMetadataEntitiesForStream(String streamName){
		ensureInitialized();
		return getMetadataEntitiesForAllStreams().get(streamName);
	}
	
	public Map<String, List<AlertStreamSchemaEntity>> getMetadataEntitiesForAllStreams(){
		ensureInitialized();
		return UnmodifiableMap.decorate(map);
	}
	
	public SortedMap<String, AlertStreamSchemaEntity> getMetadataEntityMapForStream(String streamName){
		ensureInitialized();
		return getMetadataEntityMapForAllStreams().get(streamName);
	}
	
	public Map<String, SortedMap<String, AlertStreamSchemaEntity>> getMetadataEntityMapForAllStreams(){
		ensureInitialized();
		return UnmodifiableMap.decorate(map2);
	}
}
