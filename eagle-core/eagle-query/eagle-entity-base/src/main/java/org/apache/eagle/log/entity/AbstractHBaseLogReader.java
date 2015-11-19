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

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.common.ByteUtil;
import org.apache.eagle.common.EagleBase64Wrapper;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * HBase Log Reader basic initialization:
 * <ol>
 *   <li>Open HBase connection to target HBase table</li>
 *   <li>Generate HBase filter,start and stop row key, output qualifier and Scan </li>
 *   <li><code>onOpen(HTableInterface,Scan)</code>: Callback abstract method </li>
 *   <li><code>close</code>: Close HBase connection</li>
 * </ol>
 *
 * @param <T> Reader entity class type
 *
 */
public abstract class AbstractHBaseLogReader<T> implements LogReader<T> {
	private static Logger LOG = LoggerFactory.getLogger(AbstractHBaseLogReader.class);

	protected byte[][] qualifiers;
	private HTableInterface tbl;
	private byte[] startKey;
	private byte[] stopKey;
	protected Map<String, List<String>> searchTags;
	private Filter filter;
	private Date startTime;
	private Date endTime;

//	protected ResultScanner rs;
	private boolean isOpen = false;

	/**
	 * TODO it's ugly that both _ed and prefix fields can hold prefix information,
	 * prefix field should be in precedence over _ed
	 */
	private String _prefix;
	protected EntityDefinition _ed;

	public AbstractHBaseLogReader(EntityDefinition ed, List<String> partitions, Date startTime, Date endTime,
	                              Filter filter, String lastScanKey, byte[][] outputQualifiers){
		this(ed, partitions, startTime, endTime, filter, lastScanKey, outputQualifiers, null);
	}
	/**
	 * This constructor supports partition.
	 *
	 * @param ed entity definition
	 * @param partitions partition values, which is sorted in partition definition order. TODO: in future we need to support
	 * multiple values for one partition field
	 * @param startTime start time of the query
	 * @param endTime end time of the query
	 * @param filter filter for the hbase scan
	 * @param lastScanKey the key of last scan
	 * @param outputQualifiers the bytes of output qualifier names
	 * @param prefix can be populated from outside world specifically for generic metric reader
	 */
	public AbstractHBaseLogReader(EntityDefinition ed, List<String> partitions, Date startTime, Date endTime,
	                              Filter filter, String lastScanKey, byte[][] outputQualifiers, String prefix){
		this.startTime = startTime;
		this.endTime = endTime;
		this._ed = ed;
		if (_ed.getPartitions() != null) {
			if (partitions == null || _ed.getPartitions().length != partitions.size()) {
				throw new IllegalArgumentException("Invalid argument. Entity " + ed.getClass().getSimpleName() + " defined "
						+ "partitions, but argument partitions is null or number of partition values are different!");
			}
		}
		/**
		 * decide prefix field value
		 */
		if(prefix == null || prefix.isEmpty()){
			this._prefix = _ed.getPrefix();
		}else{
			this._prefix = prefix;
		}
		this.qualifiers = outputQualifiers;
		this.filter = filter;

		this.startKey = buildRowKey(this._prefix, partitions, startTime);
		
		
		/**
		 * startTime should be inclusive, -128 is max value for hbase Bytes comparison, see PureJavaComparer.compareTo
		 * as an alternative, we can use startTime-1000 and endTime-1000 to make sure startTime is inclusive and endTime is exclusive
		 */
		this.startKey = ByteUtil.concat(this.startKey, new byte[] {-1, -1,-1,-1});
		if (lastScanKey == null) {
			this.stopKey = buildRowKey(this._prefix, partitions, endTime);
			// endTime should be exclusive
			this.stopKey = ByteUtil.concat(this.stopKey, new byte[] {-1,-1,-1,-1,-1});
		} else {
			// build stop key
			this.stopKey = EagleBase64Wrapper.decode(lastScanKey);
			// TODO to-be-fixed, probably it's an issue because contacting 1 is not
			// enough for lexicographical sorting
			this.stopKey = ByteUtil.concat(this.stopKey, new byte[] { 1 });
		}
	}
	
	/**
	 * TODO If the required field is null for a row, then this row will not be fetched. That could be a problem for counting
	 * Need another version of read to strictly get the number of rows which will return all the columns for a column family
	 */
	@Override
	public void open() throws IOException {
		if (isOpen)
			return; // silently return
		try {
			tbl = EagleConfigFactory.load().getHTable(_ed.getTable());
		} catch (RuntimeException ex) {
			throw new IOException(ex);
		}

		Scan s1 = new Scan();
		// reverse timestamp, startRow is stopKey, and stopRow is startKey
		s1.setStartRow(stopKey);
		s1.setStopRow(startKey);
		s1.setFilter(filter);
		// TODO the # of cached rows should be minimum of (pagesize and 100)
		int cs = EagleConfigFactory.load().getHBaseClientScanCacheSize();
		s1.setCaching(cs);
		// TODO not optimized for all applications
		s1.setCacheBlocks(true)
		;
		// scan specified columnfamily and qualifiers
		if(this.qualifiers == null) {
			// Filter all
			s1.addFamily(_ed.getColumnFamily().getBytes());
		}else{
			for (byte[] qualifier : qualifiers) {
				s1.addColumn(_ed.getColumnFamily().getBytes(), qualifier);
			}
		}
		// TODO: Work around https://issues.apache.org/jira/browse/HBASE-2198. More graceful implementation should use SingleColumnValueExcludeFilter, 
		// but it's complicated in current implementation. 
		workaroundHBASE2198(s1, filter);
		if (LOG.isDebugEnabled()) {
			LOG.debug(s1.toString());
		}
//		rs = tbl.getScanner(s1);
		this.onOpen(tbl,s1);
		isOpen = true;
	}

	/**
	 * HBase table connection callback function
	 *
	 * @param tbl   HBase table connection
	 * @param scan  HBase scan
	 * @throws IOException
	 */
	protected abstract void onOpen(HTableInterface tbl,Scan scan) throws IOException;

	/**
	 * <h2>History</h2>
	 * <ul>
	 * 	<li><b>Nov 19th, 2014</b>: Fix for out put all qualifiers</li>
	 * </ul>
	 * @param s1
	 * @param filter
	 */
	protected void workaroundHBASE2198(Scan s1, Filter filter) {
		if (filter instanceof SingleColumnValueFilter) {
			if(this.qualifiers == null){
				s1.addFamily(((SingleColumnValueFilter) filter).getFamily());
			}else {
				s1.addColumn(((SingleColumnValueFilter) filter).getFamily(), ((SingleColumnValueFilter) filter).getQualifier());
			}
			return;
		}
		if (filter instanceof FilterList) {
			for (Filter f : ((FilterList)filter).getFilters()) {
				workaroundHBASE2198(s1, f);
			}
		}
	}

	/**
	 * <h2>Close:</h2>
	 * 1. release current table connection
	 *
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		if(tbl != null){
			new HTableFactory().releaseHTableInterface(tbl);
		}
//		if(rs != null){
//			rs.close();
//		}
	}

	private static byte[] buildRowKey(String prefix, List<String> partitions, Date t){
		final int length = (partitions == null) ? (4 + 8) : (4 + 8 + partitions.size() * 4);
		final byte[] key = new byte[length];
		int offset = 0;
		ByteUtil.intToBytes(prefix.hashCode(), key, offset);
		offset += 4;
		if (partitions != null) {
			for (String partition : partitions) {
				ByteUtil.intToBytes(partition.hashCode(), key, offset);
				offset += 4;
			}
		}
		// reverse timestamp
		long ts = Long.MAX_VALUE - t.getTime();
		ByteUtil.longToBytes(ts, key, offset);
		return key;
	}
}
