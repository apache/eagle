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
package org.apache.eagle.log.entity.old;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.entity.InternalLog;
import org.apache.eagle.log.entity.LogReader;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.common.ByteUtil;
import org.apache.eagle.common.EagleBase64Wrapper;

public class HBaseLogReader implements LogReader {
	private static Logger LOG = LoggerFactory.getLogger(HBaseLogReader.class);

	protected byte[][] qualifiers;
	private HTableInterface tbl;
	private byte[] startKey;
	private byte[] stopKey;
	protected Map<String, List<String>> searchTags;

	private ResultScanner rs;
	private boolean isOpen = false;
	
	private Schema schema;

	public HBaseLogReader(Schema schema, Date startTime, Date endTime, 
			Map<String, List<String>> searchTags, String lastScanKey,
			byte[][] outputQualifier){
		this.schema = schema;
		this.qualifiers = outputQualifier;
		this.startKey = buildRowKey(schema.getPrefix(), startTime);
		if (lastScanKey == null) {
			this.stopKey = buildRowKey(schema.getPrefix(), endTime);
		} else {
			// build stop key
			this.stopKey = EagleBase64Wrapper.decode(lastScanKey);
			// concat byte 0 to exclude this stopKey
			this.stopKey = ByteUtil.concat(this.stopKey, new byte[] { 0 });
		}
		this.searchTags = searchTags;
	}
	
	/**
	 * TODO If the required field is null for a row, then this row will not be fetched. That could be a problem for counting
	 * Need another version of read to strictly get the number of rows which will return all the columns for a column family
	 */
	public void open() throws IOException {
		if (isOpen)
			return; // silently return
		try {
			tbl = EagleConfigFactory.load().getHTable(schema.getTable());
		} catch (RuntimeException ex) {
			throw new IOException(ex);
		}

		String rowkeyRegex = buildRegex2(searchTags);
		RegexStringComparator regexStringComparator = new RegexStringComparator(
				rowkeyRegex);
		regexStringComparator.setCharset(Charset.forName("ISO-8859-1"));
		RowFilter filter = new RowFilter(CompareOp.EQUAL, regexStringComparator);
		FilterList filterList = new FilterList();
		filterList.addFilter(filter);
		Scan s1 = new Scan();
		// reverse timestamp, startRow is stopKey, and stopRow is startKey
		s1.setStartRow(stopKey);
		s1.setStopRow(startKey);
		s1.setFilter(filterList);
		// TODO the # of cached rows should be minimum of (pagesize and 100)
		s1.setCaching(100);
		// TODO not optimized for all applications
		s1.setCacheBlocks(true);
		// scan specified columnfamily and qualifiers
		for(byte[] qualifier : qualifiers){
			s1.addColumn(schema.getColumnFamily().getBytes(), qualifier);
		}
		rs = tbl.getScanner(s1);
		isOpen = true;
	}

	public void close() throws IOException {
		if(tbl != null){
			new HTableFactory().releaseHTableInterface(tbl);
		}
		if(rs != null){
			rs.close();
		}
	}

	public void flush() throws IOException {
		tbl.flushCommits();
	}

	private byte[] buildRowKey(String appName, Date t){
		byte[] key = new byte[4 + 8];
		byte[] b = ByteUtil.intToBytes(appName.hashCode());
		System.arraycopy(b, 0, key, 0, 4);
		// reverse timestamp
		long ts = Long.MAX_VALUE - t.getTime();
		System.arraycopy(ByteUtil.longToBytes(ts), 0, key, 4, 8);
		return key;
	}

	/**
	 * one search tag may have multiple values which have OR relationship, and relationship between
	 * different search tags is AND
	 * the query is like "(TAG1=value11 OR TAG1=value12) AND TAG2=value2"
	 * @param tags
	 * @return
	 */
	protected String buildRegex2(Map<String, List<String>> tags){
		// TODO need consider that \E could be part of tag, refer to https://github.com/OpenTSDB/opentsdb/blob/master/src/core/TsdbQuery.java
		SortedMap<Integer, List<Integer>> tagHash = new TreeMap<Integer, List<Integer>>();

		for(Map.Entry<String, List<String>> entry : tags.entrySet()){
			String tagName = entry.getKey();
			List<String> stringValues = entry.getValue();
			List<Integer> hashValues = new ArrayList<Integer>(1);
			for(String value : stringValues){
				hashValues.add(value.hashCode());
			}
			tagHash.put(tagName.hashCode(), hashValues);
		}
		// <tag1:3><value1:3> ... <tagn:3><valuen:3>
		StringBuilder sb = new StringBuilder();
		sb.append("(?s)");
		sb.append("^(?:.{12})");
		sb.append("(?:.{").append(8).append("})*"); // for any number of tags
		for (Map.Entry<Integer, List<Integer>> entry : tagHash.entrySet()) {
			try {
				sb.append("\\Q");
				sb.append(new String(ByteUtil.intToBytes(entry.getKey()), "ISO-8859-1")).append("\\E");
				List<Integer> hashValues = entry.getValue();
				sb.append("(?:");
				boolean first = true;
				for(Integer value : hashValues){
					if(!first){
						sb.append('|');
					}
					sb.append("\\Q");
					sb.append(new String(ByteUtil.intToBytes(value), "ISO-8859-1"));
					sb.append("\\E");
					first = false;
				}
				sb.append(")");
				sb.append("(?:.{").append(8).append("})*"); // for any number of tags
					} catch (Exception ex) {
						LOG.error("Constructing regex error", ex);
					}
				}
				sb.append("$");
				if (LOG.isDebugEnabled()) {
					LOG.debug("Pattern is " + sb.toString());
				}
				return sb.toString();
	}
	
	public InternalLog read() throws IOException {
		if (rs == null)
			throw new IllegalArgumentException(
					"ResultScanner must be initialized before reading");

		InternalLog t = null;

		Result r = rs.next();
		if (r != null) {
			byte[] row = r.getRow();
			// skip the first 4 bytes : prefix
			long timestamp = ByteUtil.bytesToLong(row, 4);
			// reverse timestamp
			timestamp = Long.MAX_VALUE - timestamp;
			int count = 0; 
			if(qualifiers != null){
				count = qualifiers.length;
			}
			byte[][] values = new byte[count][];
			Map<String, byte[]> allQualifierValues = new HashMap<String, byte[]>();
			for (int i = 0; i < count; i++) {
				// TODO if returned value is null, it means no this column for this row, so why set null to the object?
				values[i] = r.getValue(schema.getColumnFamily().getBytes(), qualifiers[i]);
				allQualifierValues.put(new String(qualifiers[i]), values[i]);
			}
			t = buildObject(row, timestamp, allQualifierValues);
		}

		return t;
	}

	public InternalLog buildObject(byte[] row, long timestamp,
			Map<String, byte[]> allQualifierValues) {
		InternalLog alertDetail = new InternalLog();
		String myRow = EagleBase64Wrapper.encodeByteArray2URLSafeString(row);
		alertDetail.setEncodedRowkey(myRow);
		alertDetail.setPrefix(schema.getPrefix());
		alertDetail.setSearchTags(searchTags);
		alertDetail.setTimestamp(timestamp);

		Map<String, byte[]> logQualifierValues = new HashMap<String, byte[]>();
		Map<String, String> logTags = new HashMap<String, String>();
		for (Map.Entry<String, byte[]> entry : allQualifierValues.entrySet()) {
			if (schema.isTag(entry.getKey())) {
				if (entry.getValue() != null) {
					logTags.put(entry.getKey(), new String(entry.getValue()));
				}
			} else {
				logQualifierValues.put(entry.getKey(),entry.getValue());
			}
		}
		alertDetail.setQualifierValues(logQualifierValues);
		alertDetail.setTags(logTags);
		return alertDetail;
	}
}
