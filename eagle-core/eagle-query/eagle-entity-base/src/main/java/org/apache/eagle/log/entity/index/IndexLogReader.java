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

import org.apache.eagle.log.entity.LogReader;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

public abstract class IndexLogReader implements LogReader {

	// TODO: Work around https://issues.apache.org/jira/browse/HBASE-2198. More graceful implementation should use SingleColumnValueExcludeFilter, 
	// but it's complicated in current implementation. 
	protected static void workaroundHBASE2198(Get get, Filter filter,byte[][] qualifiers) {
		if (filter instanceof SingleColumnValueFilter) {
			if(qualifiers == null) {
				get.addFamily(((SingleColumnValueFilter) filter).getFamily());
			}else{
				get.addColumn(((SingleColumnValueFilter) filter).getFamily(), ((SingleColumnValueFilter) filter).getQualifier());
			}
			return;
		}
		if (filter instanceof FilterList) {
			for (Filter f : ((FilterList)filter).getFilters()) {
				workaroundHBASE2198(get, f,qualifiers);
			}
		}
	}

}
