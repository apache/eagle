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

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public class HBaseLogReader2 extends AbstractHBaseLogReader<InternalLog> {
	protected ResultScanner rs;

	public HBaseLogReader2(EntityDefinition ed, List<String> partitions, Date startTime, Date endTime, Filter filter, String lastScanKey, byte[][] outputQualifiers) {
		super(ed, partitions, startTime, endTime, filter, lastScanKey, outputQualifiers);
	}

	/**
	 * This constructor supports partition.
	 *
	 * @param ed               entity definition
	 * @param partitions       partition values, which is sorted in partition definition order. TODO: in future we need to support
	 *                         multiple values for one partition field
	 * @param startTime        start time of the query
	 * @param endTime          end time of the query
	 * @param filter           filter for the hbase scan
	 * @param lastScanKey      the key of last scan
	 * @param outputQualifiers the bytes of output qualifier names
	 * @param prefix           can be populated from outside world specifically for generic metric reader
	 */
	public HBaseLogReader2(EntityDefinition ed, List<String> partitions, Date startTime, Date endTime, Filter filter, String lastScanKey, byte[][] outputQualifiers, String prefix) {
		super(ed, partitions, startTime, endTime, filter, lastScanKey, outputQualifiers, prefix);
	}

	@Override
	protected void onOpen(HTableInterface tbl, Scan scan) throws IOException {
		rs = tbl.getScanner(scan);
	}

	/**
	 * <h2>Close:</h2>
	 * 1. Call super.close(): release current table connection <br></br>
	 * 2. Close Scanner<br></br>
	 *
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		super.close();
		if(rs != null){
			rs.close();
		}
	}

	@Override
	public InternalLog read() throws IOException {
		if (rs == null)
			throw new IllegalArgumentException(
					"ResultScanner must be initialized before reading");
		InternalLog t = null;
		Result r = rs.next();
		if (r != null) {
			t = HBaseInternalLogHelper.parse(_ed, r, qualifiers);
		}
		return t;
	}
}
