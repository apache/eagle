/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.topology.entity;

import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.topology.TopologyConstants;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("hadoop_topology")
@ColumnFamily("f")
@Prefix("mrservicestatus")
@Service(TopologyConstants.MR_INSTANCE_SERVICE_NAME)
@TimeSeries(false)
public class MRServiceTopologyAPIEntity  extends TopologyBaseAPIEntity {
	@Column("status")
	private String status;
	@Column("numConfiguredMapSlots")
	private String numConfiguredMapSlots;
	@Column("numConfiguredReduceSlots")
	private String numConfiguredReduceSlots;
	@Column("healthReport")
	private String healthReport;
	
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
		valueChanged("status");
	}
	public String getNumConfiguredMapSlots() {
		return numConfiguredMapSlots;
	}
	public void setNumConfiguredMapSlots(String numConfiguredMapSlots) {
		this.numConfiguredMapSlots = numConfiguredMapSlots;
		valueChanged("numConfiguredMapSlots");
	}
	public String getNumConfiguredReduceSlots() {
		return numConfiguredReduceSlots;
	}
	public void setNumConfiguredReduceSlots(String numConfiguredReduceSlots) {
		this.numConfiguredReduceSlots = numConfiguredReduceSlots;
		valueChanged("numConfiguredReduceSlots");
	}
	public String getHealthReport() {
		return healthReport;
	}
	public void setHealthReport(String healthReport) {
		this.healthReport = healthReport;
		valueChanged("healthReport");
	}
	
}
