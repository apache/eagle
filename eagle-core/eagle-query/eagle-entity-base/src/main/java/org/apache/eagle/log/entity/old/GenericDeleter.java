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
import java.util.*;

import org.apache.eagle.common.EagleBase64Wrapper;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.HBaseInternalLogHelper;
import org.apache.eagle.log.entity.meta.EntityConstants;
import org.apache.eagle.log.entity.InternalLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.meta.IndexDefinition;

public class GenericDeleter {
	private static final Logger LOG = LoggerFactory.getLogger(GenericDeleter.class);

	private final HBaseLogDeleter deleter;
	private final HBaseLogByRowkeyReader reader;
	
	
	public GenericDeleter(EntityDefinition ed) {
		this(ed.getTable(), ed.getColumnFamily());
	}
	
	public GenericDeleter(String table, String columnFamily) {
		this.deleter = new HBaseLogDeleter(table, columnFamily);
		this.reader = new HBaseLogByRowkeyReader(table, columnFamily, true, null);
	}

    public void deleteByRowkeys(List<byte[]> rowkeys) throws Exception {
        try {
            deleter.open();
            deleter.deleteRowkeys(rowkeys);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw e;
        }
    }

    public void deleteByEncodedRowkeys(List<String> encodedRowkeys) throws Exception {
        try {
            deleter.open();
            deleter.deleteRowByRowkey(encodedRowkeys);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw e;
        }
    }
	
	public List<String> delete(List<? extends TaggedLogAPIEntity> entities) throws Exception{
        List<String> encodedRowkey = new LinkedList<String>();
		try{
			deleter.open();
			final Map<Class<? extends TaggedLogAPIEntity>, List<TaggedLogAPIEntity>> entityClassMap = classifyEntities(entities);
			for (Map.Entry<Class<? extends TaggedLogAPIEntity>, List<TaggedLogAPIEntity>> entry : entityClassMap.entrySet()) {
				final Class<? extends TaggedLogAPIEntity> clazz = entry.getKey();
				final List<? extends TaggedLogAPIEntity> entityList = entry.getValue();

				final EntityDefinition entityDef = EntityDefinitionManager.getEntityDefinitionByEntityClass(clazz);
				// TODO: we should fix this hardcoded prefix hack
				fixPrefixAndTimestampIssue(entityList, entityDef);

				final List<byte[]> rowkeys = RowkeyHelper.getRowkeysByEntities(entityList, entityDef);
				// Check index
				final IndexDefinition[] indexes = entityDef.getIndexes();
				if (indexes != null && indexes.length > 0) {
					reader.open();
					final List<InternalLog> logs = reader.get(rowkeys);
					final List<TaggedLogAPIEntity> newEntities = HBaseInternalLogHelper.buildEntities(logs, entityDef);
					for (TaggedLogAPIEntity entity : newEntities) {
						// Add index rowkeys
						for (IndexDefinition index : indexes) {
							final byte[] indexRowkey = index.generateIndexRowkey(entity);
							rowkeys.add(indexRowkey);
						}
					}
				}
                for(byte[] rowkey:rowkeys) {
                    encodedRowkey.add(EagleBase64Wrapper.encodeByteArray2URLSafeString(rowkey));
                }
				deleter.deleteRowkeys(rowkeys);
			}
		}catch(IOException ioe){
			LOG.error("Fail writing tagged log", ioe);
			throw ioe;
		}finally{
			deleter.close();
	 	}
        return encodedRowkey;
	}

	private void fixPrefixAndTimestampIssue(List<? extends TaggedLogAPIEntity> entities, EntityDefinition entityDef) {
		for (TaggedLogAPIEntity e : entities) {
			e.setPrefix(entityDef.getPrefix());
			if (!entityDef.isTimeSeries()) {
				e.setTimestamp(EntityConstants.FIXED_WRITE_TIMESTAMP); // set timestamp to MAX, then actually stored 0
			}
		}
	}

	private Map<Class<? extends TaggedLogAPIEntity>, List<TaggedLogAPIEntity>> classifyEntities(List<? extends TaggedLogAPIEntity> entities) {
		final Map<Class<? extends TaggedLogAPIEntity>, List<TaggedLogAPIEntity>> result = new 
				HashMap<Class<? extends TaggedLogAPIEntity>, List<TaggedLogAPIEntity>>();
		for (TaggedLogAPIEntity entity : entities) {
			final Class<? extends TaggedLogAPIEntity> clazz = entity.getClass();
			List<TaggedLogAPIEntity> list = result.get(clazz);
			if (list == null) {
				list = new ArrayList<TaggedLogAPIEntity>();
				result.put(clazz, list);
			}
			list.add(entity);
		}
		return result;
	}
}
