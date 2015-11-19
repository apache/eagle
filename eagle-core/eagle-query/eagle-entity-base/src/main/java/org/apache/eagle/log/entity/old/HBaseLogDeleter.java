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
import java.util.ArrayList;
import java.util.List;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;

import org.apache.eagle.log.entity.InternalLog;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.common.EagleBase64Wrapper;

public class HBaseLogDeleter implements LogDeleter{
	private HTableInterface tbl;
	private String table;
	private String columnFamily;
	
	public HBaseLogDeleter(String table, String columnFamily) {
		this.table = table;
		this.columnFamily = columnFamily;
	}
	
	@Override
	public void open() throws IOException {
		try{
			tbl = EagleConfigFactory.load().getHTable(this.table);
		}catch(RuntimeException ex){
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
	public void flush() throws IOException{
		throw new IllegalArgumentException("Not supported flush for hbase delete");
	}
	
	/**
	 * support delete by constructing a rowkey or by encoded rowkey passed from client
	 */
	@Override
	public void delete(InternalLog log) throws IOException{
		final byte[] rowkey = RowkeyHelper.getRowkey(log);
		final Delete delete = createDelete(rowkey);
		tbl.delete(delete);
	}
	
	public void delete(TaggedLogAPIEntity entity, EntityDefinition entityDef) throws Exception {
		final byte[] rowkey = RowkeyHelper.getRowkey(entity, entityDef);
		final Delete delete = createDelete(rowkey);
		tbl.delete(delete);
	}
	
	/**
	 * Batch delete
	 * @param logs
	 * @throws IOException
	 */
	public void delete(List<InternalLog> logs) throws IOException{
		final List<byte[]> rowkeys = RowkeyHelper.getRowkeysByLogs(logs);
		deleteRowkeys(rowkeys);
	}


	/**
	 * Batch delete
	 * @throws Exception
	 */
	public void deleteEntities(List<? extends TaggedLogAPIEntity> entities, EntityDefinition entityDef) throws Exception{
		final List<byte[]> rowkeys = RowkeyHelper.getRowkeysByEntities(entities, entityDef);
		deleteRowkeys(rowkeys);
	}
	
	/**
	 * Batch delete
	 * @throws IOException
	 */
	public void deleteRowkeys(List<byte[]> rowkeys) throws IOException {
		final List<Delete> deletes = new ArrayList<Delete>(rowkeys.size());
		for (byte[] rowkey : rowkeys) {
			final Delete delete = createDelete(rowkey);
			deletes.add(delete);
		}
		tbl.delete(deletes);
	}
	
	@Override
	public void deleteRowByRowkey(String encodedRowkey) throws IOException{
		byte[] row = EagleBase64Wrapper.decode(encodedRowkey);
		final Delete delete = createDelete(row);
		tbl.delete(delete);
	}

	public void deleteRowByRowkey(List<String> encodedRowkeys) throws IOException {
		final List<byte[]> rowkeys = RowkeyHelper.getRowkeysByEncodedRowkeys(encodedRowkeys);
		deleteRowkeys(rowkeys);
	}
	
	private Delete createDelete(byte[] row) throws IOException{
		Delete delete = new Delete(row);
		delete.deleteFamily(columnFamily.getBytes());
		return delete;
	}

}
