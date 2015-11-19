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

import org.apache.eagle.query.parser.ORExpression;
import org.apache.hadoop.hbase.filter.Filter;

import java.util.List;
import java.util.Map;

/**
 * search condition includes the following:
 * 1. prefix - part of rowkey
 * 2. startTime,endTime - timestamp, part of rowkey
 * 3. hbase filter converted from query 
 * 4. aggregate parameters
 * 4. sort options
 * 5. output fields and tags
 * 6. entityName
 * 7. pagination: pageSize and startRowkey
 */
public class SearchCondition{
	private String startTime;
	private String endTime;
	private Filter filter;
	private List<String> outputFields;
	private boolean outputAll;
	private long pageSize;
	private String startRowkey;
	private String entityName;
	private List<String> partitionValues;
	private ORExpression queryExpression;

	public boolean isOutputVerbose() {
		return outputVerbose;
	}

	public void setOutputVerbose(boolean outputVerbose) {
		this.outputVerbose = outputVerbose;
	}

	public Map<String, String> getOutputAlias() {
		return outputAlias;
	}

	public void setOutputAlias(Map<String, String> outputAlias) {
		this.outputAlias = outputAlias;
	}

	private boolean outputVerbose;
	private Map<String,String> outputAlias;

	/**
	 * copy constructor
	 * @param sc
	 */
	public SearchCondition(SearchCondition sc){
		this.startTime = sc.startTime;
		this.endTime = sc.endTime;
		this.filter = sc.filter;
		this.outputFields = sc.outputFields;
		this.pageSize = sc.pageSize;
		this.startRowkey = sc.startRowkey;
		this.entityName = sc.entityName;
		this.partitionValues = sc.partitionValues;
		this.queryExpression = sc.queryExpression;
	}
	
	public SearchCondition(){
	}
	
	public Filter getFilter() {
		return filter;
	}
	public void setFilter(Filter filter) {
		this.filter = filter;
	}
	public long getPageSize() {
		return pageSize;
	}
	public void setPageSize(long pageSize) {
		this.pageSize = pageSize;
	}
	public String getStartRowkey() {
		return startRowkey;
	}
	public void setStartRowkey(String startRowkey) {
		this.startRowkey = startRowkey;
	}
	public String getEntityName() {
		return entityName;
	}
	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}
	public List<String> getOutputFields() {
		return outputFields;
	}
	public void setOutputFields(List<String> outputFields) {
		this.outputFields = outputFields;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public List<String> getPartitionValues() {
		return partitionValues;
	}
	public void setPartitionValues(List<String> partitionValues) {
		this.partitionValues = partitionValues;
	}
	public ORExpression getQueryExpression() {
		return queryExpression;
	}
	public void setQueryExpression(ORExpression queryExpression) {
		this.queryExpression = queryExpression;
	}

	public boolean isOutputAll() {
		return outputAll;
	}

	public void setOutputAll(boolean outputAll) {
		this.outputAll = outputAll;
	}
}
