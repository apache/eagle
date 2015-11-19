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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseLogWriter implements LogWriter {
	private static Logger LOG = LoggerFactory.getLogger(HBaseLogWriter.class);
	private static byte[] EMPTY_INDEX_QUALIFER_VALUE = "".getBytes();
	
	private HTableInterface tbl;
	private String table;
	private String columnFamily;
	
	public HBaseLogWriter(String table, String columnFamily) {
		// TODO assert for non-null of table and columnFamily
		this.table = table;
		this.columnFamily = columnFamily;
	}
	
	@Override
	public void open() throws IOException {
		try{
			tbl = EagleConfigFactory.load().getHTable(this.table);
//			LOGGER.info("HBase table " + table + " audo reflush is " + (tbl.isAutoFlush() ? "enabled" : "disabled"));
		}catch(Exception ex){
			LOG.error("Cannot create htable", ex);
			throw new IOException(ex);
		}
	}

	@Override
	public void close() throws IOException {
		if(tbl != null){
			new HTableFactory().releaseHTableInterface(tbl);
		}
	}

	@Override
	public void flush() throws IOException {
		tbl.flushCommits();
	}
	
	protected void populateColumnValues(Put p, InternalLog log){
		Map<String, byte[]> qualifierValues = log.getQualifierValues();
		// iterate all qualifierValues
		for(Map.Entry<String, byte[]> entry : qualifierValues.entrySet()){
			p.add(columnFamily.getBytes(), entry.getKey().getBytes(), entry.getValue());
		}
		
		Map<String, String> tags = log.getTags();
		// iterate all tags, each tag will be stored as a column qualifier
		if(tags != null){
			for(Map.Entry<String, String> entry : tags.entrySet()){
				// TODO need a consistent handling of null values
				if(entry.getValue() != null)
					p.add(columnFamily.getBytes(), entry.getKey().getBytes(), entry.getValue().getBytes());
			}
		}
	}

	/**
	 * TODO need think about if multi-PUT is necessary, by checking if autoFlush works
	 */
	@Override
	public byte[] write(InternalLog log) throws IOException{
		final byte[] rowkey = RowkeyBuilder.buildRowkey(log);
		final Put p = new Put(rowkey);
		populateColumnValues(p, log);
		tbl.put(p);
		final List<byte[]> indexRowkeys = log.getIndexRowkeys();
		if (indexRowkeys != null) {
			writeIndexes(rowkey, indexRowkeys);
		}
		return rowkey;
	}

	/**
	 * TODO need think about if multi-PUT is necessary, by checking if autoFlush works
	 */
	public List<byte[]> write(List<InternalLog> logs) throws IOException{
		final List<Put> puts = new ArrayList<Put>(logs.size());
		final List<byte[]> result = new ArrayList<byte[]>(logs.size());
		for (InternalLog log : logs) {
			final byte[] rowkey = RowkeyBuilder.buildRowkey(log);
			final Put p = new Put(rowkey);
			populateColumnValues(p, log);
			puts.add(p);
			final List<byte[]> indexRowkeys = log.getIndexRowkeys();
			if (indexRowkeys != null) {
				writeIndexes(rowkey, indexRowkeys, puts);
			}
			result.add(rowkey);
		}
		tbl.put(puts);
		return result;
	}
	
	@Override
	public void updateByRowkey(byte[] rowkey, InternalLog log) throws IOException{
		Put p = new Put(rowkey);
		populateColumnValues(p, log);
		tbl.put(p);
		final List<byte[]> indexRowkeys = log.getIndexRowkeys();
		if (indexRowkeys != null) {
			writeIndexes(rowkey, indexRowkeys);
		}
	}

	private void writeIndexes(byte[] rowkey, List<byte[]> indexRowkeys) throws IOException {
		for (byte[] indexRowkey : indexRowkeys) {
			Put p = new Put(indexRowkey);
			p.add(columnFamily.getBytes(), rowkey, EMPTY_INDEX_QUALIFER_VALUE);
			tbl.put(p);
		}
	}

	private void writeIndexes(byte[] rowkey, List<byte[]> indexRowkeys, List<Put> puts) throws IOException {
		for (byte[] indexRowkey : indexRowkeys) {
			Put p = new Put(indexRowkey);
			p.add(columnFamily.getBytes(), rowkey, EMPTY_INDEX_QUALIFER_VALUE);
			puts.add(p);
//			tbl.put(p);
		}
	}

	
}
