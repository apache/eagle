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
package org.apache.eagle.log.entity.index;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.entity.HBaseInternalLogHelper;
import org.apache.eagle.log.entity.InternalLog;
import org.apache.eagle.log.entity.meta.IndexDefinition;
import org.apache.eagle.common.ByteUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class NonClusteredIndexLogReader extends IndexLogReader {
	private final IndexDefinition indexDef;
	private final List<byte[]> indexRowkeys;
	private final byte[][] qualifiers;
	private final Filter filter;
	private HTableInterface tbl;
	private boolean isOpen = false;
	private Result[] results;
	private int index = -1;
	private final List<Scan> scans;
	private int currentScanIndex = 0;
	private ResultScanner currentResultScanner;

	// Max tag key/value. 
	private static final byte[] MAX_TAG_VALUE_BYTES = {(byte) 0XFF,(byte) 0XFF,(byte) 0XFF,(byte) 0XFF,(byte) 0XFF,(byte) 0XFF,(byte) 0XFF,(byte) 0XFF,(byte) 0XFF};
	private static final int BATCH_MULTIGET_SIZE = 1000;

	public NonClusteredIndexLogReader(IndexDefinition indexDef, List<byte[]> indexRowkeys, byte[][] qualifiers, Filter filter) {
		this.indexDef = indexDef;
		this.indexRowkeys = indexRowkeys;
		this.qualifiers = qualifiers;
		this.filter = filter;
		this.scans = buildScans();
	}
	

	private List<Scan> buildScans() {
		final ArrayList<Scan> result = new ArrayList<Scan>(indexRowkeys.size());
		for (byte[] rowkey : indexRowkeys) {
			Scan s = new Scan();
			s.setStartRow(rowkey);
			// In rowkey the tag key/value is sorted by the hash code of the key, so MAX_TAG_VALUE_BYTES is enough as the end key
			final byte[] stopRowkey = ByteUtil.concat(rowkey, MAX_TAG_VALUE_BYTES);
			s.setStopRow(stopRowkey);
			// TODO the # of cached rows should be minimum of (pagesize and 100)
			int cs = EagleConfigFactory.load().getHBaseClientScanCacheSize();
			s.setCaching(cs);
			// TODO not optimized for all applications
			s.setCacheBlocks(true);
			// scan specified columnfamily for all qualifiers
			s.addFamily(indexDef.getEntityDefinition().getColumnFamily().getBytes());
			result.add(s);
		}
		return result;
	}

	@Override
	public void open() throws IOException {
		if (isOpen)
			return; // silently return
		try {
			tbl = EagleConfigFactory.load().getHTable(indexDef.getEntityDefinition().getTable());
		} catch (RuntimeException ex) {
			throw new IOException(ex);
		}
		currentScanIndex = 0;
		openNewScan();
		fillResults();
	}

	private boolean openNewScan() throws IOException {
		closeCurrentScanResult();
		if (currentScanIndex >= scans.size()) {
			return false;
		}
		final Scan scan = scans.get(currentScanIndex++);
		currentResultScanner = tbl.getScanner(scan);
		return true;
	}

	private void fillResults() throws IOException {
		if (currentResultScanner == null) {
			return;
		}
		index = 0;
		int count = 0;
		Result r = null;
        final List<Get> gets = new ArrayList<Get>(BATCH_MULTIGET_SIZE);
		final byte[] family = indexDef.getEntityDefinition().getColumnFamily().getBytes();
		while (count < BATCH_MULTIGET_SIZE) {
			r = currentResultScanner.next();
			if (r == null) {
				if (openNewScan()) {
					continue;
				} else {
					break;
				}
			}
			for (byte[] rowkey : r.getFamilyMap(family).keySet()) {
				if (rowkey.length == 0) {	// invalid rowkey
					continue;
				}
				final Get get = new Get(rowkey);
                if (filter != null) {
                	get.setFilter(filter);
                }
				if(qualifiers != null) {
					for (int j = 0; j < qualifiers.length; ++j) {
						// Return the specified qualifiers
						get.addColumn(family, qualifiers[j]);
					}
				}else {
					get.addFamily(family);
				}
        		workaroundHBASE2198(get, filter,qualifiers);
				gets.add(get);
				++count;
			}
		}
		if (count == 0) {
			results = null;
			return;
		}
		results = tbl.get(gets);
		if (results == null || results.length == 0) {
			fillResults();
		}
	}


	private void closeCurrentScanResult() {
		if (currentResultScanner != null) {
			currentResultScanner.close();
			currentResultScanner = null;
		}
	}


	@Override
	public void close() throws IOException {
		if(tbl != null){
			new HTableFactory().releaseHTableInterface(tbl);
		}
		closeCurrentScanResult();
	}

	@Override
	public InternalLog read() throws IOException {
		if (tbl == null) {
			throw new IllegalArgumentException("Haven't open before reading");
		}
		
		Result r = null;
		InternalLog t = null;
		while ((r = getNextResult()) != null) {
			if (r.getRow() == null) {
				continue;
			}
			t = HBaseInternalLogHelper.parse(indexDef.getEntityDefinition(), r, qualifiers);
			break;
		}
		return t;
	}


	private Result getNextResult() throws IOException {
		if (results == null || results.length == 0 || index >= results.length) {
			fillResults();
		}
		if (results == null || results.length == 0 || index >= results.length) {
			return null;
		}
		return results[index++];
	}
	
}
