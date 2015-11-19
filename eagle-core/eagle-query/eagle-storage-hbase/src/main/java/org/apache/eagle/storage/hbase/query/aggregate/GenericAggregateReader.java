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
package org.apache.eagle.storage.hbase.query.aggregate;

import org.apache.eagle.log.entity.AbstractHBaseLogReader;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.query.aggregate.AggregateCondition;
import org.apache.eagle.storage.hbase.query.coprocessor.impl.AggregateClientImpl;
import org.apache.eagle.query.aggregate.raw.GroupbyKeyValue;
import org.apache.eagle.storage.hbase.query.coprocessor.AggregateClient;
import org.apache.eagle.storage.hbase.query.coprocessor.AggregateResult;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * @since : 11/7/14,2014
 */
public class GenericAggregateReader extends AbstractHBaseLogReader<List<GroupbyKeyValue>> {
	private final long startTime;
	private final long endTime;
	private AggregateClient aggregateClient = new AggregateClientImpl();
	private EntityDefinition ed;
	private final AggregateCondition aggregateCondition;
	private AggregateResult result;

	/**
	 *
	 * @param ed                Entity Definition
	 * @param partitions        Partition values
	 * @param startTime         Start time
	 * @param endTime           End time
	 * @param filter            HBase filter for scanning
	 * @param lastScanKey       Last HBase scan row key in String
	 * @param outputQualifiers  HBase output qualifiers in bytes
	 * @param condition         GroupAggregateCondition Object
	 *
	 * @see org.apache.eagle.query.aggregate.AggregateCondition
	 */
	@SuppressWarnings("unused")
	private GenericAggregateReader(EntityDefinition ed,
	                                List<String> partitions,
	                                Date startTime,
	                                Date endTime,
	                                Filter filter,
	                                String lastScanKey,
	                                byte[][] outputQualifiers,
	                                AggregateCondition condition) {
		super(ed, partitions, startTime, endTime, filter, lastScanKey, outputQualifiers);
		this.ed = ed;
		this.startTime = startTime.getTime();
		this.endTime = endTime.getTime();
		this.aggregateCondition = condition;
	}

	/**
	 *
	 * @param ed                Entity Definition
	 * @param partitions        Partition values
	 * @param startTime         Start time
	 * @param endTime           End time
	 * @param filter            HBase filter for scanning
	 * @param lastScanKey       Last HBase scan row key in String
	 * @param outputQualifiers  HBase output qualifiers in bytes
	 * @param prefix            HBase prefix, not necessary except for GenericMetric query
	 * @param condition         GroupAggregateCondition Object
	 *
	 * @see org.apache.eagle.query.aggregate.AggregateCondition
	 */
	public GenericAggregateReader(EntityDefinition ed,
	                               List<String> partitions,
	                               Date startTime,
	                               Date endTime,
	                               Filter filter,
	                               String lastScanKey,
	                               byte[][] outputQualifiers,
	                               String prefix,
	                               AggregateCondition condition) {
		super(ed, partitions, startTime, endTime, filter, lastScanKey, outputQualifiers, prefix);
		this.ed = ed;
		this.startTime = startTime.getTime();
		this.endTime = endTime.getTime();
		this.aggregateCondition = condition;
	}

	@Override
	protected void onOpen(HTableInterface tbl, Scan scan) throws IOException {
		this.result = this.aggregateClient.aggregate(
				tbl,
				this.ed,
				scan,
				this.aggregateCondition.getGroupbyFields(),
				this.aggregateCondition.getAggregateFunctionTypes(),
				this.aggregateCondition.getAggregateFields(),
				this.aggregateCondition.isTimeSeries(),
				this.startTime,
				this.endTime,
				this.aggregateCondition.getIntervalMS());
	}

	@Override
	public List<GroupbyKeyValue> read() throws IOException {
		return this.result.getKeyValues();
	}

	public long getFirstTimestamp() {
		return this.result.getStartTimestamp();
	}

	public long getLastTimestamp() {
		return this.result.getStopTimestamp();
	}
}