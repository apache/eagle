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
package org.apache.eagle.log.entity.test;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.Column;
import org.apache.eagle.log.entity.meta.ColumnFamily;
import org.apache.eagle.log.entity.meta.TimeSeries;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import org.apache.eagle.log.entity.meta.Prefix;
import org.apache.eagle.log.entity.meta.Table;

/**
 * this class is written by customer, but it has some contracts
 * 0. This class should conform to java bean conventions
 * 1. Annotate this class with hbase table name
 * 2. Annotate this class with hbase column family name
 * 3. Annotate those qualifier fields with column name
 * 4. Fire property change event for all fields' setter method, where field name is mandatory parameter
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("alertdetail")
@ColumnFamily("f")
@Prefix("hadoop")
@TimeSeries(true)
public class TestEntity extends TaggedLogAPIEntity {
	@Column("remediationID")
	private String remediationID;
	@Column("remediationStatus")
	private String remediationStatus;
	@Column("c")
	private long count;
	@Column("d")
	private int numHosts;
	@Column("e")
	private Long numClusters;

	public Long getNumClusters() {
		return numClusters;
	}

	public void setNumClusters(Long numClusters) {
		this.numClusters = numClusters;
		_pcs.firePropertyChange("numClusters", null, null);
	}

	public int getNumHosts() {
		return numHosts;
	}

	public void setNumHosts(int numHosts) {
		this.numHosts = numHosts;
		_pcs.firePropertyChange("numHosts", null, null);
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
		_pcs.firePropertyChange("count", null, null);
	}

	public String getRemediationID() {
		return remediationID;
	}

	public void setRemediationID(String remediationID) {
		this.remediationID = remediationID;
		_pcs.firePropertyChange("remediationID", null, null);
	}

	public String getRemediationStatus() {
		return remediationStatus;
	}

	public void setRemediationStatus(String remediationStatus) {
		this.remediationStatus = remediationStatus;
		_pcs.firePropertyChange("remediationStatus", null, null);
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(super.toString());
		sb.append(", remediationID:");
		sb.append(remediationID);
		sb.append(", remediationStatus:");
		sb.append(remediationStatus);
		return sb.toString();
	}
}