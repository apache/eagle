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
package org.apache.eagle.service.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericEntityBatchReader;
import org.apache.eagle.log.entity.RowkeyBuilder;
import org.apache.eagle.log.entity.SearchCondition;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.query.ListQueryCompiler;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.common.EagleBase64Wrapper;

/**
 * Support stream based entity read. Internally it splits entity fetching to multiple threads to improve 
 * the performance. However, it doesn't support multi-threading for client to read entities from result set.
 * 
 */
public class SplitFullScanEntityReader<ENTITY extends TaggedLogAPIEntity> {

	// class members
	public static final int DEFAULT_BUFFER_SIZE = 10 * 1000;
	public static final int MAX_WRITE_TIME_OUT_IN_SECONDS = 60;
	private static final Logger LOG = LoggerFactory.getLogger(SplitFullScanEntityReader.class);
	private static final TaggedLogAPIEntity COMPLETED_ENTITY = new TaggedLogAPIEntity();

	// instance members
	private final int splits;
	private final String query;
	private final long startTime;
	private final long endTime;
	private final String startRowkey;
	private final int pageSize;
	private final int bufferSize;
	
	public SplitFullScanEntityReader(String query,
			String startTime, String endTime,
			int splits, String startRowkey, int pageSize) {
		this(
				query, 
				DateTimeUtil.humanDateToSecondsWithoutException(startTime) * 1000,
				DateTimeUtil.humanDateToSecondsWithoutException(endTime) * 1000,
				splits,
				startRowkey,
				pageSize
			);
	}
	
	public SplitFullScanEntityReader(String query, long startTime, long endTime,
			int splits, String startRowkey, int pageSize) {
		this(query, startTime, endTime, splits, startRowkey, pageSize, 
				DEFAULT_BUFFER_SIZE);
	}
	
	public SplitFullScanEntityReader(String query, long startTime, long endTime,
			int splits, String startRowkey, int pageSize, int bufferSize) {
		this.query = query;
		this.startTime = startTime;
		this.endTime = endTime;
		this.splits = splits;
		this.startRowkey = startRowkey;
		this.pageSize = pageSize;
		this.bufferSize = bufferSize;
	}
	
	public EntityResultSet<ENTITY> read() throws Exception {
		final EntityResultSet<ENTITY> resultSet = new EntityResultSet<ENTITY>(new ArrayBlockingQueue<TaggedLogAPIEntity>(bufferSize));
		final List<GenericEntityBatchReader> readers = createSplitThreads();
		
		final int size = readers.size();
		if (size > 0) {
			final AtomicInteger threadCount = new AtomicInteger(size);
			final AtomicInteger entityCount = new AtomicInteger(0);
			for (GenericEntityBatchReader reader : readers) {
				final EntityFetchThread<ENTITY> thread = new EntityFetchThread<ENTITY>(reader, threadCount, entityCount, resultSet);
				thread.start();
			}
		} else {
			resultSet.getQueue().add(COMPLETED_ENTITY);
		}
		return resultSet;
	}

	protected List<GenericEntityBatchReader> createSplitThreads() throws Exception {
		
		final List<GenericEntityBatchReader> readers = new ArrayList<GenericEntityBatchReader>();
		final ListQueryCompiler comp = new ListQueryCompiler(query);
		final EntityDefinition entityDef = EntityDefinitionManager.getEntityByServiceName(comp.serviceName());
		if (entityDef == null) {
			throw new IllegalArgumentException("Invalid entity name: " + comp.serviceName());
		}
		
		// TODO: For now we don't support one query to query multiple partitions. In future 
		// if partition is defined for the entity, internally We need to spawn multiple
		// queries and send one query for each search condition for each partition
		final List<String[]> partitionValues = comp.getQueryPartitionValues();
		partitionConstraintValidate(partitionValues, query);
		
		long lastTimestamp = Long.MAX_VALUE;
		if (startRowkey != null) {
			final byte[] lastRowkey = EagleBase64Wrapper.decode(startRowkey);
			lastTimestamp = RowkeyBuilder.getTimestamp(lastRowkey, entityDef);
		}
		
		final long duration = (endTime - startTime) / splits;
		for (int i = 0; i < splits; ++i) {
			
			final long slotStartTime = startTime + (i * duration);
			if (slotStartTime > lastTimestamp) {
				// ignore this slot
				continue;
			}
			final long slotEndTime = startTime + ((i + 1) * duration);
			final SearchCondition condition = new SearchCondition();
			final String slotStartTimeString = DateTimeUtil.secondsToHumanDate(slotStartTime / 1000);
			final String slotEndTimeString = DateTimeUtil.secondsToHumanDate(slotEndTime / 1000);
			condition.setStartTime(slotStartTimeString);
			condition.setEndTime(slotEndTimeString);
			
			condition.setFilter(comp.filter());
			condition.setQueryExpression(comp.getQueryExpression());
			if (partitionValues != null) {
				condition.setPartitionValues(Arrays.asList(partitionValues.get(0)));
			}
			// Should be careful to the startRowkey setting. Only set startRowkey when 
			// lastTimestamp is within the slot time range.
			if (startRowkey != null && lastTimestamp >= startTime && lastTimestamp < endTime) {
				condition.setStartRowkey(startRowkey);
			}
			condition.setPageSize(pageSize);
			
			if (comp.hasAgg()) {
				List<String> groupbyFields = comp.groupbyFields();
				List<String> outputFields = new ArrayList<String>();
				if(groupbyFields != null){
					outputFields.addAll(groupbyFields);
				}
				outputFields.addAll(comp.aggregateFields());
				condition.setOutputFields(outputFields);
			} else {
				condition.setOutputFields(comp.outputFields());
			}
			readers.add(new GenericEntityBatchReader(comp.serviceName(), condition));
		}
		return readers;
	}
	

	private static void partitionConstraintValidate(List<String[]> partitionValues, String query) {
		if (partitionValues != null && partitionValues.size() > 1) {
			final String[] values = partitionValues.get(0);
			for (int i = 1; i < partitionValues.size(); ++i) {
				final String[] tmpValues = partitionValues.get(i);
				for (int j = 0; j < values.length; ++j) {
					if (values[j] == null || (!values[j].equals(tmpValues[j]))) {
						final String errMsg = "One query for multiple partitions is NOT allowed for now! Query: " + query;
						LOG.error(errMsg);
						throw new IllegalArgumentException(errMsg);
					}
				}
			}
		}
	}
	
	
	@SuppressWarnings("unchecked")
	public static class EntityResultSet<ENTITY extends TaggedLogAPIEntity> {
		private static final long DEFAULT_TIMEOUT_IN_MS = 1000;

		private boolean fetchCompleted = false;
		private final BlockingQueue<TaggedLogAPIEntity> queue;
		private volatile Exception ex = null;

		public EntityResultSet(BlockingQueue<TaggedLogAPIEntity> queue) {
			this.queue = queue;
		}
		
		public boolean hasMoreData() {
			return queue.size() > 0 || (!fetchCompleted);
		}
		
		public ENTITY next(long timeout, TimeUnit unit) throws InterruptedException {
			if (fetchCompleted) {
				return null;
			}
			final TaggedLogAPIEntity entity = queue.poll(timeout, unit);
			if (COMPLETED_ENTITY.equals(entity)) {
				fetchCompleted = true;
				return null;
			}
			return (ENTITY)entity;
		}
		
		public ENTITY next() throws Exception {
			TaggedLogAPIEntity entity = null;
			while (!fetchCompleted) {
				try {
					entity = queue.poll(DEFAULT_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);
					if (COMPLETED_ENTITY.equals(entity)) {
						fetchCompleted = true;
						if (ex != null) {
							throw ex;
						}
						return null;
					}
					if (entity != null) {
						return (ENTITY)entity;
					}
				} catch (InterruptedException ex) {
					// Just ignore
				}
			}
			return null;
		}
		
		void setException(Exception ex) {
			this.ex = ex;
		}
		
		BlockingQueue<TaggedLogAPIEntity> getQueue() {
			return queue;
		}
	}
	
	private static class EntityFetchThread<ENTITY extends TaggedLogAPIEntity> extends Thread {
		
		private final GenericEntityBatchReader reader;
		private final AtomicInteger threadCount;
		private final AtomicInteger entityCount;
		private final EntityResultSet<ENTITY> resultSet;
		
		private EntityFetchThread(GenericEntityBatchReader reader, AtomicInteger threadCount, AtomicInteger entityCount, EntityResultSet<ENTITY> resultSet) {
			this.reader = reader;
			this.threadCount = threadCount;
			this.entityCount = entityCount;
			this.resultSet = resultSet;
		}
		
	    @Override
	    public void run() {
	    	try {
	    		final List<ENTITY> entities = reader.read();
	    		entityCount.addAndGet(entities.size());
	    		for (ENTITY entity : entities) {
	    			if (!resultSet.getQueue().offer(entity, MAX_WRITE_TIME_OUT_IN_SECONDS, TimeUnit.SECONDS)) {
	    				resultSet.setException(new IOException("Write entity to queue timeout"));
		    			resultSet.getQueue().add(COMPLETED_ENTITY);
	    			}
	    		}
	    	} catch (Exception ex) {
				resultSet.setException(ex);
    			resultSet.getQueue().add(COMPLETED_ENTITY);
	    	} finally {
	    		final int count = threadCount.decrementAndGet();
	    		if (count == 0) {
	    			resultSet.getQueue().add(COMPLETED_ENTITY);
	    			LOG.info("Total fetched " + entityCount.get() + " entities");
	    		}
	    	}
	    }
	}
}
