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
@Prefix("hoststatus")
@Service(TopologyConstants.HDFS_INSTANCE_SERVICE_NAME)
@TimeSeries(false)
public class HdfsServiceTopologyAPIEntity extends TopologyBaseAPIEntity {
	@Column("status")
	private String status;
	@Column("configuredCapacityTB")
	private String configuredCapacityTB;
	@Column("usedCapacityTB")
	private String usedCapacityTB;
	@Column("numBlocks")
	private String numBlocks;
	@Column("numFailedVolumes")
	private String numFailedVolumes;
	@Column("remediationStatus")
	private String remediationStatus;

	public String getRemediationStatus() {
		return remediationStatus;
	}
	public void setRemediationStatus(String remediationStatus) {
		this.remediationStatus = remediationStatus;
		valueChanged("remediationStatus");
	}
	public String getNumFailedVolumes() {
		return numFailedVolumes;
	}
	public void setNumFailedVolumes(String numFailedVolumes) {
		this.numFailedVolumes = numFailedVolumes;
		valueChanged("numFailedVolumes");
	}
	public String getNumBlocks() {
		return numBlocks;
	}
	public void setNumBlocks(String numBlocks) {
		this.numBlocks = numBlocks;
		valueChanged("numBlocks");
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
		valueChanged("status");
	}
	public String getConfiguredCapacityTB() {
		return configuredCapacityTB;
	}
	public void setConfiguredCapacityTB(String configuredCapacityTB) {
		this.configuredCapacityTB = configuredCapacityTB;
		valueChanged("configuredCapacityTB");
	}
	public String getUsedCapacityTB() {
		return usedCapacityTB;
	}
	public void setUsedCapacityTB(String usedCapacityTB) {
		this.usedCapacityTB = usedCapacityTB;
		valueChanged("usedCapacityTB");
	}
}
