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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.base.taggedlog.NoSuchRowException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;

import org.apache.eagle.log.entity.InternalLog;
import org.apache.eagle.common.ByteUtil;
import org.apache.eagle.common.EagleBase64Wrapper;

/**
 * Get details of rowkey and qualifiers given a raw rowkey. This function mostly is used for inspecting one row's content 
 * This only supports single column family, which is mostly used in log application 
 */
public class HBaseLogByRowkeyReader implements Closeable{
	private String table;
	private String columnFamily;
	private byte[][] outputQualifiers;
	private boolean includingAllQualifiers;
	private HTableInterface tbl;
	private boolean isOpen;
	
	/**
	 * if includingAllQualifiers is true, then the fourth argument outputQualifiers is ignored
	 * if includingAllQualifiers is false, then need calculate based on the fourth argument outputQualifiers
	 */
	public HBaseLogByRowkeyReader(String table, String columnFamily, boolean includingAllQualifiers, List<String> qualifiers){
		this.table = table;
		this.columnFamily = columnFamily;
		if(qualifiers != null){
			this.outputQualifiers = new byte[qualifiers.size()][];
			int i = 0;
			for(String qualifier : qualifiers){
				this.outputQualifiers[i++] = qualifier.getBytes();
			}
		}
		this.includingAllQualifiers = includingAllQualifiers;
	}
	
	
	public void open() throws IOException {
		if (isOpen)
			return; // silently return
		try {
			tbl = EagleConfigFactory.load().getHTable(this.table);
		} catch (RuntimeException ex) {
			throw new IOException(ex);
		}
		
		isOpen = true;
	}

	/**
	 * Here all qualifiers' values goes into qualifierValues of InternalLog as given a row, we can't differentiate it's a tag or a field
	 * @param rowkeys
	 * @return
	 * @throws IOException
	 */
	public List<InternalLog> get(List<byte[]> rowkeys) throws IOException, NoSuchRowException {
		final List<Get> gets = createGets(rowkeys);
		final Result[] results = tbl.get(gets);
		final List<InternalLog> logs = new ArrayList<InternalLog>();
		for (Result result : results) {
			final InternalLog log = buildLog(result);
			logs.add(log);
		}
		return logs;
	}
	
	private List<Get> createGets(List<byte[]> rowkeys) {
		final List<Get> gets = new ArrayList<Get>();
		for (byte[] rowkey : rowkeys) {
			final Get get = createGet(rowkey);
			gets.add(get);
		}
		return gets;
	}


	private Get createGet(byte[] rowkey) {
		final Get get = new Get(rowkey);
		byte[] cf = this.columnFamily.getBytes();
		if(includingAllQualifiers){
			get.addFamily(cf);
		}else{
			for(byte[] outputQualifier : outputQualifiers){
				get.addColumn(cf, outputQualifier);
			}
		}
		return get;
	}


	/**
	 * Here all qualifiers' values goes into qualifierValues of InternalLog as given a row, we can't differentiate it's a tag or a field
	 * @param rowkey
	 * @return
	 * @throws IOException
	 */
	public InternalLog get(byte[] rowkey) throws IOException, NoSuchRowException{
		final Get get = createGet(rowkey);
		final Result result = tbl.get(get);
		final InternalLog log = buildLog(result);
		return log;
	}
	
	private InternalLog buildLog(Result result) {
		final InternalLog log = new InternalLog();
		final byte[] rowkey = result.getRow();
		log.setEncodedRowkey(EagleBase64Wrapper.encodeByteArray2URLSafeString(rowkey));
		long timestamp = ByteUtil.bytesToLong(rowkey, 4);
		timestamp = Long.MAX_VALUE - timestamp;
		log.setTimestamp(timestamp);
		Map<String, byte[]> qualifierValues = new HashMap<String, byte[]>();
		log.setQualifierValues(qualifierValues);
		NavigableMap<byte[], byte[]> map = result.getFamilyMap(this.columnFamily.getBytes());
		if(map == null){
			throw new NoSuchRowException(EagleBase64Wrapper.encodeByteArray2URLSafeString(rowkey));
		}
		for(Map.Entry<byte[], byte[]> entry : map.entrySet()){
			byte[] qualifier = entry.getKey();
			byte[] value = entry.getValue();
			qualifierValues.put(new String(qualifier), value);
		}
		return log;
	}


	public void close() throws IOException {
		if(tbl != null){
			new HTableFactory().releaseHTableInterface(tbl);
		}
	}
}
