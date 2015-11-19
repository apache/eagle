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
package org.apache.eagle.query.aggregate.raw;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;

import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

public class GroupbyKeyComparator implements Comparator<GroupbyKey>{
	@Override 
    public int compare(GroupbyKey key1, GroupbyKey key2){
		List<BytesWritable> list1 = key1.getValue();
		List<BytesWritable> list2 = key2.getValue();
		
		if(list1 == null || list2 == null || list1.size() != list2.size())
			throw new IllegalArgumentException("2 list of groupby fields must be non-null and have the same size");
		ListIterator<BytesWritable> e1 = list1.listIterator();
		ListIterator<BytesWritable> e2 = list2.listIterator();
		while(e1.hasNext() && e2.hasNext()){
			int r = Bytes.compareTo(e1.next().copyBytes(), e2.next().copyBytes());
			if(r != 0)
				return r;
		}
		return 0;
	}
}
