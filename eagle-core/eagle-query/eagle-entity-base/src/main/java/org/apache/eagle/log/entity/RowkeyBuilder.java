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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.eagle.log.entity.meta.EntityConstants;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.common.ByteUtil;

public class RowkeyBuilder {
	
	public static final int EMPTY_PARTITION_DEFAULT_HASH_CODE = 0;
	
	/**
	 * Generate the internal sorted hashmap for tags. Please note the partition tags should not be included in the result map.
	 * @param partitions array of partition tags in order
	 * @param tags tags of the entity
	 * @return the sorted hash map of the tags
	 */
	public static SortedMap<Integer, Integer> generateSortedTagMap(String[] partitions, Map<String, String> tags) {
		final SortedMap<Integer, Integer> tagHashMap = new TreeMap<Integer, Integer>();
		for (Map.Entry<String, String> entry: tags.entrySet()) {
			final String tagName = entry.getKey();
			final String tagValue = entry.getValue();
			// If it's a partition tag, we need to remove it from tag hash list. It need to 
			// put to the fix partition hash slot in rowkey.
			if (tagValue == null || isPartitionTag(partitions, tagName))
				continue;
			tagHashMap.put(tagName.hashCode(), tagValue.hashCode());
		}
		return tagHashMap;
	}
	
	/**
	 * build rowkey from InternalLog object
	 * @param log internal log entity to write
	 * @return the rowkey of the entity
	 */
	public static byte[] buildRowkey(InternalLog log) {
		final String[] partitions = log.getPartitions();
		final Map<String, String> tags = log.getTags();
		final SortedMap<Integer, Integer> tagHashMap = generateSortedTagMap(partitions, tags);
		
		// reverse timestamp
		long ts = Long.MAX_VALUE - log.getTimestamp();
		
		List<Integer> partitionHashValues = new ArrayList<Integer>();
		if (partitions != null) {
			for (String partition : partitions) {
				final String tagValue = tags.get(partition);
				if (tagValue != null) {
					partitionHashValues.add(tagValue.hashCode());
				} else {
					partitionHashValues.add(EMPTY_PARTITION_DEFAULT_HASH_CODE);
				}
			}
		}
		return buildRowkey(log.getPrefix().hashCode(), partitionHashValues, ts, tagHashMap);
	}
	
	public static long getTimestamp(byte[] rowkey, EntityDefinition ed) {
		if (!ed.isTimeSeries()) {
			return EntityConstants.FIXED_WRITE_TIMESTAMP;
		}
		final int offset = (ed.getPartitions() == null) ? 4 : (4 + ed.getPartitions().length * 4);
		return Long.MAX_VALUE - ByteUtil.bytesToLong(rowkey, offset);
	}
	
	/**
	 * Check if the tagName is one of the partition tags
	 * @param partitions paritition tags of the entity
	 * @param tagName the tag name that needs to check
	 * @return
	 */
	private static boolean isPartitionTag(String[] partitions, String tagName) {
		if (partitions != null) {
			for (String partition : partitions) {
				if (partition.equals(tagName)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * rowkey is: prefixHash:4 + (partitionValueHash:4)* + timestamp:8 + (tagnameHash:4 + tagvalueHash:4)*
	 * partition fields are sorted by partition definition order, while tag fields are sorted by tag name's 
	 * hash code values. 
	 */
	private static byte[] buildRowkey(int prefixHash, List<Integer> partitionHashValues, long timestamp, SortedMap<Integer, Integer> tags){
		// alloacate byte array for rowkey
		final int len = 4 + 8 + tags.size() * (4 + 4) + (partitionHashValues.size() * 4);
		final byte[] rowkey = new byte[len];
		int offset = 0;

		// 1. set prefix
		ByteUtil.intToBytes(prefixHash, rowkey, offset);
		offset += 4;
		
		// 2. set partition
		for (Integer partHash : partitionHashValues) {
			ByteUtil.intToBytes(partHash, rowkey, offset);
			offset += 4;
		}
		
		// 3. set timestamp
		ByteUtil.longToBytes(timestamp, rowkey, offset);
		offset += 8;

		// 4. set tag key/value hashes
		for (Map.Entry<Integer, Integer> entry : tags.entrySet()) {
			ByteUtil.intToBytes(entry.getKey(), rowkey, offset);
			offset += 4;
			ByteUtil.intToBytes(entry.getValue(), rowkey, offset);
			offset += 4;
		}
		
		return rowkey;
	}
}
