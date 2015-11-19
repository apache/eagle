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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.entity.HBaseInternalLogHelper;
import org.apache.eagle.log.entity.InternalLog;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;

import org.apache.eagle.log.entity.meta.EntityDefinition;

public class RowKeyLogReader extends IndexLogReader {
	private final EntityDefinition ed;
	private final List<byte[]> rowkeys;
    private final byte[][] qualifiers;
    private HTableInterface tbl;
	private boolean isOpen = false;
	private Result[] entityResult;
    private int getIndex = -1;

    public RowKeyLogReader(EntityDefinition ed, byte[] rowkey) {
        this.ed = ed;
        this.rowkeys = new ArrayList<>();
        this.rowkeys.add(rowkey);
        this.qualifiers = null;
    }

	public RowKeyLogReader(EntityDefinition ed, byte[] rowkey,byte[][] qualifiers) {
		this.ed = ed;
		this.rowkeys = new ArrayList<>();
        this.rowkeys.add(rowkey);
        this.qualifiers = qualifiers;
	}

	public RowKeyLogReader(EntityDefinition ed, List<byte[]> rowkeys,byte[][] qualifiers) {
		this.ed = ed;
		this.rowkeys = rowkeys;
        this.qualifiers = qualifiers;
	}

	@Override
	public void open() throws IOException {
		if (isOpen)
			return; // silently return
		try {
			tbl = EagleConfigFactory.load().getHTable(ed.getTable());
		} catch (RuntimeException ex) {
			throw new IOException(ex);
		}
		final byte[] family = ed.getColumnFamily().getBytes();
        List<Get> gets = new ArrayList<>(this.rowkeys.size());

        for(byte[] rowkey:rowkeys) {
            Get get = new Get(rowkey);
            get.addFamily(family);

            if(qualifiers != null) {
                for(byte[] qualifier: qualifiers){
                    get.addColumn(family,qualifier);
                }
            }

            gets.add(get);
        }

        entityResult = tbl.get(gets);
		isOpen = true;
	}

	@Override
	public void close() throws IOException {
		if(tbl != null){
			new HTableFactory().releaseHTableInterface(tbl);
		}
	}

	@Override
	public InternalLog read() throws IOException {
        if(entityResult == null || entityResult.length == 0 || this.getIndex >= entityResult.length - 1){
            return null;
        }
        getIndex ++;
		InternalLog t = HBaseInternalLogHelper.parse(ed, entityResult[getIndex], this.qualifiers);
		return t;
	}
}