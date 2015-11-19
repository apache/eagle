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
package org.apache.eagle.log.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.common.DateTimeUtil;

/**
 * multi-threading stream readers which only applies to time-series entity where we split the query into
 * different time range
 * 
 * When this class is used together with list query or aggregate query, be aware that the query's behavior could
 * be changed for example pageSize does not work well, output sequence is not determined
 */
public class GenericEntityStreamReaderMT extends StreamReader{
	private static final Logger LOG = LoggerFactory.getLogger(GenericEntityStreamReaderMT.class);
	private List<GenericEntityStreamReader> readers = new ArrayList<GenericEntityStreamReader>(); 
	
	public GenericEntityStreamReaderMT(String serviceName, SearchCondition condition, int numThreads) throws Exception{
		checkIsTimeSeries(serviceName);
		checkNumThreads(numThreads);
		long queryStartTime = DateTimeUtil.humanDateToSeconds(condition.getStartTime())*1000;
		long queryEndTime = DateTimeUtil.humanDateToSeconds(condition.getEndTime())*1000;
		long subStartTime = queryStartTime;
		long subEndTime = 0;
		long interval = (queryEndTime-queryStartTime) / numThreads;
		for(int i=0; i<numThreads; i++){
			// split search condition by time range
			subStartTime = queryStartTime + i*interval;
			if(i == numThreads-1){
				subEndTime = queryEndTime;
			}else{
				subEndTime = subStartTime + interval;
			}
			String strStartTime = DateTimeUtil.millisecondsToHumanDateWithSeconds(subStartTime);
			String strEndTime = DateTimeUtil.millisecondsToHumanDateWithSeconds(subEndTime);
			SearchCondition sc = new SearchCondition(condition);
			sc.setStartTime(strStartTime);
			sc.setEndTime(strEndTime);
			GenericEntityStreamReader reader = new GenericEntityStreamReader(serviceName, sc);
			readers.add(reader);
		}
	}
	
	private void checkIsTimeSeries(String serviceName) throws Exception{
		EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName(serviceName);
		if(!ed.isTimeSeries()){
			throw new IllegalArgumentException("Multi-threading stream reader must be applied to time series table");
		}
	}
	
	private void checkNumThreads(int numThreads){
		if(numThreads <= 0){
			throw new IllegalArgumentException("Multi-threading stream reader must have numThreads >= 1");
		}
	}
	
	/**
	 * default to 2 threads
	 * @param serviceName
	 * @param condition
	 */
	public GenericEntityStreamReaderMT(String serviceName, SearchCondition condition) throws Exception{
		this(serviceName, condition, 2);
	}
	
	@Override
	public void readAsStream() throws Exception{
		// populate listeners to all readers
		for(EntityCreationListener l : _listeners){
			for(GenericEntityStreamReader r : readers){
				r.register(l);
			}
		}

		List<Future<Void>> futures = new ArrayList<Future<Void>>();
		for(GenericEntityStreamReader r : readers){
			SingleReader reader = new SingleReader(r);
			Future<Void> readFuture = EagleConfigFactory.load().getExecutor().submit(reader);
			futures.add(readFuture);
		}
		
		// join threads and check exceptions
		for(Future<Void> future : futures){
			try{
				future.get();
			}catch(Exception ex){
				LOG.error("Error in read", ex);
				throw ex;
			}
		}
	}
	
	private static class SingleReader implements Callable<Void>{
		private GenericEntityStreamReader reader;
		public SingleReader(GenericEntityStreamReader reader){
			this.reader = reader;
		}
		@Override
		public Void call() throws Exception{
			reader.readAsStream();
			return null;
		}
	}

	@Override
	public long getLastTimestamp() {
		long lastTimestamp = 0;
		for (GenericEntityStreamReader reader : readers) {
			if (lastTimestamp < reader.getLastTimestamp()) {
				lastTimestamp = reader.getLastTimestamp();
			}
		}
		return lastTimestamp;
	}

	@Override
	public long getFirstTimestamp() {
		long firstTimestamp = 0;
		for (GenericEntityStreamReader reader : readers) {
			if (firstTimestamp > reader.getLastTimestamp() || firstTimestamp == 0) {
				firstTimestamp = reader.getLastTimestamp();
			}
		}
		return firstTimestamp;
	}
}
