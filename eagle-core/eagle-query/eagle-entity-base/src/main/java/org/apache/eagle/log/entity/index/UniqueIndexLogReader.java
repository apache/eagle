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
import java.util.NavigableMap;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.entity.HBaseInternalLogHelper;
import org.apache.eagle.log.entity.InternalLog;
import org.apache.eagle.log.entity.meta.IndexDefinition;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;

public class UniqueIndexLogReader extends IndexLogReader {

	private final IndexDefinition indexDef;
	private final List<byte[]> indexRowkeys; 
	private final byte[][] qualifiers;
	private final Filter filter;
	private HTableInterface tbl;
	private boolean isOpen = false;
	private Result[] entityResults;
	private int index = -1;

	public UniqueIndexLogReader(IndexDefinition indexDef, List<byte[]> indexRowkeys, byte[][] qualifiers, Filter filter) {
		this.indexDef = indexDef;
		this.indexRowkeys = indexRowkeys;
		this.qualifiers = qualifiers;
		this.filter = filter;
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
		final byte[] family = indexDef.getEntityDefinition().getColumnFamily().getBytes();
        final List<Get> indexGets = new ArrayList<>();
        for (byte[] rowkey : indexRowkeys) {
            Get get = new Get(rowkey);
            // Return all index qualifiers
            get.addFamily(family);
            indexGets.add(get);
        }
        final Result[] indexResults = tbl.get(indexGets);
        indexGets.clear();
        for (Result indexResult : indexResults) {
        	final NavigableMap<byte[], byte[]> map = indexResult.getFamilyMap(family);
        	if (map == null) {
        		continue;
        	}
        	for (byte[] entityRowkey : map.keySet()) {
                Get get = new Get(entityRowkey);
                if (filter != null) {
                	get.setFilter(filter);
                }
				if(qualifiers == null) {
					// filter all qualifiers if output qualifiers are null
					get.addFamily(family);
				}else {
					for (int i = 0; i < qualifiers.length; ++i) {
						// Return the specified qualifiers
						get.addColumn(family, qualifiers[i]);
					}
				}
				workaroundHBASE2198(get, filter,qualifiers);
        		indexGets.add(get);
        	}
        }
        entityResults = tbl.get(indexGets);
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
		if (entityResults == null) {
			throw new IllegalArgumentException("entityResults haven't been initialized before reading");
		}
		InternalLog t = null;
		while (entityResults.length > ++index) {
			Result r = entityResults[index];
			if (r != null) {
				if (r.getRow() == null) {
					continue;
				}
				t = HBaseInternalLogHelper.parse(indexDef.getEntityDefinition(), r, qualifiers);
				break;
			}
		}
		return t;
	}

}
