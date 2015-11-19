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
package org.apache.eagle.log.entity.meta;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.RowkeyBuilder;
import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.query.parser.ANDExpression;
import org.apache.eagle.query.parser.AtomicExpression;
import org.apache.eagle.query.parser.ComparisonOperator;
import org.apache.eagle.query.parser.ORExpression;
import org.apache.eagle.common.ByteUtil;

/**
 * Eagle index schema definition.
 * 
 * 1. Index schema can be defined in entity class by annotation.
 * 2. One index schema can contain multiple fields/tags, defined in order
 * 3. We only support immutable indexing for now
 * 4. When entity is created or deleted, the corresponding index entity should be created or deleted at the same time
 * 5. Index transparency to queries. Queries go through index when and only when index can serve all search conditions after query rewrite
 * 
 *
 */
public class IndexDefinition {
	
	public enum IndexType {
		UNIQUE_INDEX,
		NON_CLUSTER_INDEX,
		NON_INDEX
	}

	private final EntityDefinition entityDef;
	private final Index index;
	private final IndexColumn[] columns;
	private final String indexPrefix;
	
	private static final byte[] EMPTY_VALUE = new byte[0];
	private static final Charset UTF_8_CHARSET = Charset.forName("UTF-8");
	public static final int EMPTY_PARTITION_DEFAULT_HASH_CODE = 0;
	public static final int MAX_INDEX_VALUE_BYTE_LENGTH = 65535;
	
	private static final String FIELD_NAME_PATTERN_STRING = "^@(.*)$";
	private static final Pattern FIELD_NAME_PATTERN = Pattern.compile(FIELD_NAME_PATTERN_STRING);
	private final static Logger LOG = LoggerFactory.getLogger(IndexDefinition.class);

	public IndexDefinition(EntityDefinition entityDef, Index index) {
		this.entityDef = entityDef;
		this.index = index;
		this.indexPrefix = entityDef.getPrefix() + "_" + index.name();
		final String[] indexColumns = index.columns();
		this.columns = new IndexColumn[indexColumns.length];
		for (int i = 0; i < indexColumns.length; ++i) {
			final String name = indexColumns[i];
			final boolean isTag = entityDef.isTag(name);
			final Qualifier qualifier = isTag ? null : entityDef.getDisplayNameMap().get(name);
			columns[i] = new IndexColumn(name, isTag, qualifier);
		}
		LOG.info("Created index " + index.name() + " for " + entityDef.getEntityClass().getSimpleName());
	}

	public EntityDefinition getEntityDefinition() {
		return entityDef;
	}
	
	public Index getIndex() {
		return index;
	}
	
	public String getIndexName() {
		return index.name();
	}
	
	public IndexColumn[] getIndexColumns() {
		return columns;
	}
	
	public String getIndexPrefix() {
		return indexPrefix;
	}
	
	public boolean isUnique() {
		return index.unique();
	}
	
	/**
	 * Check if the query is suitable to go through index. If true, then return the value of index fields in order. Otherwise return null.
	 * TODO: currently index fields should be string type.
	 * 
	 * @param query query expression after re-write
	 * @param rowkeys if the query can go through the index, all rowkeys will be added into rowkeys.
	 * @return true if the query can go through the index, otherwise return false
	 */
	public IndexType canGoThroughIndex(ORExpression query, List<byte[]> rowkeys) {
		if (query == null || query.getANDExprList() == null || query.getANDExprList().isEmpty()) 
			return IndexType.NON_CLUSTER_INDEX;
		if (rowkeys != null) {
			rowkeys.clear();
		}
		final Map<String, String> indexfieldMap = new HashMap<String, String>();
		for(ANDExpression andExpr : query.getANDExprList()) {
			indexfieldMap.clear();
			for(AtomicExpression ae : andExpr.getAtomicExprList()) {
				// TODO temporarily ignore those fields which are not for attributes
				final String fieldName = parseEntityAttribute(ae.getKey());
				if(fieldName != null && ComparisonOperator.EQUAL.equals(ae.getOp())){
					indexfieldMap.put(fieldName, ae.getValue());
				}
			}
			final String[] partitions = entityDef.getPartitions();
			int[] partitionValueHashs = null;
			if (partitions != null) {
				partitionValueHashs = new int[partitions.length];
				for (int i = 0; i < partitions.length; ++i) {
					final String value = indexfieldMap.get(partitions[i]);
					if (value == null) {
						throw new IllegalArgumentException("Partition " + partitions[i] + " is not defined in the query: " + query.toString());
					}
					partitionValueHashs[i] = value.hashCode();
				}
			}
			final byte[][] indexFieldValues = new byte[columns.length][];
			for (int i = 0; i < columns.length; ++i) {
				final IndexColumn col = columns[i];
				if (!indexfieldMap.containsKey(col.getColumnName())) {
					// If we have to use scan anyway, there's no need to go through index
					return IndexType.NON_INDEX;
				}
				final String value = indexfieldMap.get(col.getColumnName());
				indexFieldValues[i] = value.getBytes();
			}
			final byte[] rowkey = generateUniqueIndexRowkey(indexFieldValues, partitionValueHashs, null);
			if (rowkeys != null) {
				rowkeys.add(rowkey);
			}
		}
		if (index.unique()) {
			return IndexType.UNIQUE_INDEX;
		}
		return IndexType.NON_CLUSTER_INDEX;
	}

	private String parseEntityAttribute(String fieldName) {
		Matcher m = FIELD_NAME_PATTERN.matcher(fieldName);
		if(m.find()){
			return m.group(1);
		}
		return null;
	}
	
	// TODO: We should move index rowkey generation later since this class is for general purpose, not only for hbase.
	public byte[] generateIndexRowkey(TaggedLogAPIEntity entity) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		if (entity.getClass() != entityDef.getEntityClass()) {
			throw new IllegalArgumentException("Expected entity class: " + entityDef.getEntityClass().getName() + ", but got class " + entity.getClass().getName());
		}
		final byte[][] indexValues = generateIndexValues(entity);
		final int[] partitionHashCodes = generatePartitionHashCodes(entity);
		SortedMap<Integer, Integer> tagMap = null;
		if (!index.unique()) {
			// non cluster index
			tagMap = RowkeyBuilder.generateSortedTagMap(entityDef.getPartitions(), entity.getTags());
		}
		
		return generateUniqueIndexRowkey(indexValues, partitionHashCodes, tagMap);
	}
	
	private byte[] generateUniqueIndexRowkey(byte[][] indexValues, int[] partitionHashCodes, SortedMap<Integer, Integer> tagMap) {
		final int prefixHashCode = indexPrefix.hashCode();
		int totalLength = 4;
		totalLength += (partitionHashCodes != null) ? (4 * partitionHashCodes.length) : 0;
		
		totalLength += (2 * indexValues.length);
		for (int i = 0; i < indexValues.length; ++i) {
			final byte[] value = indexValues[i];
			totalLength += value.length;
		}
		if (tagMap != null && (!tagMap.isEmpty())) {
			totalLength += tagMap.size() * 8;
		}
		
		int offset = 0;
		final byte[] rowkey = new byte[totalLength];
		
		// 1. set prefix
		ByteUtil.intToBytes(prefixHashCode, rowkey, offset);
		offset += 4;
		
		// 2. set partition
		if (partitionHashCodes != null) {
			for (Integer partitionHashCode : partitionHashCodes) {
				ByteUtil.intToBytes(partitionHashCode, rowkey, offset);
				offset += 4;
			}
		}
		
		// 3. set index values
		for (int i = 0; i < columns.length; ++i) {
			ByteUtil.shortToBytes((short)indexValues[i].length, rowkey, offset);
			offset += 2;
			for (int j = 0; j < indexValues[i].length; ++j) {
				rowkey[offset++] = indexValues[i][j];
			}
		}
		
		// Check if it's non clustered index, then set the tag/value hash code
		if (tagMap != null && (!tagMap.isEmpty())) {
			// 4. set tag key/value hashes
			for (Map.Entry<Integer, Integer> entry : tagMap.entrySet()) {
				ByteUtil.intToBytes(entry.getKey(), rowkey, offset);
				offset += 4;
				ByteUtil.intToBytes(entry.getValue(), rowkey, offset);
				offset += 4;
			}
		}
		
		return rowkey;
	}

	private int[] generatePartitionHashCodes(TaggedLogAPIEntity entity) {
		final String[] partitions = entityDef.getPartitions();
		int[] result = null;
		if (partitions != null) {
			result = new int[partitions.length];
			final Map<String, String> tags = entity.getTags();
			for (int i = 0 ; i < partitions.length; ++i) {
				final String partition = partitions[i];
				final String tagValue = tags.get(partition);
				if (tagValue != null) {
					result[i] = tagValue.hashCode();
				} else {
					result[i] = EMPTY_PARTITION_DEFAULT_HASH_CODE;
				}
			}
		}
		return result;
	}

	private byte[][] generateIndexValues(TaggedLogAPIEntity entity) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {

		final byte[][] result = new byte[columns.length][];
		for (int i = 0; i < columns.length; ++i) {
			final IndexColumn column = columns[i];
			final String columnName = column.getColumnName();
			if (column.isTag) {
				final Map<String, String> tags = entity.getTags();
				if (tags == null || tags.get(columnName) == null) {
					result[i] = EMPTY_VALUE;
				} else {
					result[i] = tags.get(columnName).getBytes(UTF_8_CHARSET);
				}
			} else {
				PropertyDescriptor pd = column.getPropertyDescriptor();
				if (pd == null) {
					pd = PropertyUtils.getPropertyDescriptor(entity, columnName);
					column.setPropertyDescriptor(pd);
				}
				final Object value = pd.getReadMethod().invoke(entity);
				if (value == null) {
					result[i] = EMPTY_VALUE;
				} else {
					final Qualifier q = column.getQualifier();
					result[i] = q.getSerDeser().serialize(value);
				}
			}
			if (result[i].length > MAX_INDEX_VALUE_BYTE_LENGTH) {
				throw new IllegalArgumentException("Index field value exceeded the max length: " + MAX_INDEX_VALUE_BYTE_LENGTH + ", actual length: " + result[i].length);
			}
		}
		return result;
	}
	
	/**
	 * Index column definition class
	 *
	 */
	public static class IndexColumn {
		private final String columnName;
		private final boolean isTag;
		private final Qualifier qualifier;
		private PropertyDescriptor propertyDescriptor;
		
		public IndexColumn(String columnName, boolean isTag, Qualifier qualifier) {
			this.columnName = columnName;
			this.isTag = isTag;
			this.qualifier = qualifier;
		}
		
		public String getColumnName() {
			return columnName;
		}
		public boolean isTag() {
			return isTag;
		}
		
		public Qualifier getQualifier() {
			return qualifier;
		}

		public PropertyDescriptor getPropertyDescriptor() {
			return propertyDescriptor;
		}

		public void setPropertyDescriptor(PropertyDescriptor propertyDescriptor) {
			this.propertyDescriptor = propertyDescriptor;
		}
		
	}
}
