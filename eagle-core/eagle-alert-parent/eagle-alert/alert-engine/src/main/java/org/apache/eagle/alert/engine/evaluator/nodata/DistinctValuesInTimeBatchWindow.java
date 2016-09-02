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
package org.apache.eagle.alert.engine.evaluator.nodata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.eagle.alert.engine.model.StreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistinctValuesInTimeBatchWindow {

	private static final Logger LOG = LoggerFactory.getLogger(DistinctValuesInTimeBatchWindow.class);
	
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	
	// wisb (what is should be) set for expected full set value of multiple columns
	@SuppressWarnings("rawtypes")
	private volatile Set wisb = new HashSet();
	
	private NoDataPolicyTimeBatchHandler handler;
	
	/**
	 * map from value to max timestamp for this value
	 */
	private Map<Object, Long> valueMaxTimeMap = new HashMap<>();
	
	private long startTime = -1;
	private long nextEmitTime = -1;
	private long timeInMilliSeconds;

	public DistinctValuesInTimeBatchWindow(NoDataPolicyTimeBatchHandler handler, 
			long timeInMilliSeconds, @SuppressWarnings("rawtypes") Set wisb) {
		this.handler = handler;
		this.timeInMilliSeconds = timeInMilliSeconds;
		if (wisb != null) {
			this.wisb = wisb;
		}
	}

	public Map<Object, Long> distinctValues() {
		return valueMaxTimeMap;
	}
	
	public void send(StreamEvent event, Object value, long timestamp) {
		synchronized(this) {
			if (startTime < 0) {
				startTime = System.currentTimeMillis();
				
				scheduler.scheduleAtFixedRate(new Runnable() {

					@SuppressWarnings({ "unchecked", "rawtypes" })
					@Override
					public void run() {
						try {
							LOG.info("{}/{}: {}", startTime, nextEmitTime, valueMaxTimeMap.keySet());
							synchronized (valueMaxTimeMap) {
								boolean sendAlerts = false;
								
								if (nextEmitTime < 0) {
									nextEmitTime = startTime + timeInMilliSeconds;
								}
								
								if (System.currentTimeMillis() > nextEmitTime) {
									startTime = nextEmitTime;
									nextEmitTime += timeInMilliSeconds;
									sendAlerts = true;
								} else {
									sendAlerts = false;
								}
								
								if (sendAlerts) {
									// alert
									handler.compareAndEmit(wisb, distinctValues().keySet(), event);
									LOG.info("alert for wiri: {} compares to wisb: {}", distinctValues().keySet(), wisb);
									
									if (distinctValues().keySet().size() > 0) {
										wisb = new HashSet(distinctValues().keySet());
									}
									valueMaxTimeMap.clear();
									LOG.info("Clear wiri & update wisb to {}", wisb);
								}
							}
						} catch (Throwable t) {
							LOG.error("failed to run batch window for gap alert", t);
						}
					}
					
				}, 0, timeInMilliSeconds / 2, TimeUnit.MILLISECONDS);
			}
		}
		
		if (valueMaxTimeMap.containsKey(value)) {
			// remove that entry with old timestamp in timeSortedMap
			long oldTime = valueMaxTimeMap.get(value);
			if (oldTime >= timestamp) {
				// no any effect as the new timestamp is equal or even less than
				// old timestamp
				return;
			}
		}
		// update new timestamp in valueMaxTimeMap
		valueMaxTimeMap.put(value, timestamp);
		
		LOG.info("sent: {} with start: {}/next: {}", valueMaxTimeMap.keySet(), startTime, nextEmitTime);
	}

}
