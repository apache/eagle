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

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.topology.TopologyConstants;
import org.codehaus.jackson.map.annotate.JsonSerialize;


/**
 * Entity model for the node status.
 * 
 * Tags:
 * cluster
 * datacenter.
 * hostname
 * 
 * 
 * @author xinzli
 *
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("hadoop_topology")
@ColumnFamily("f")
@Prefix("nodestatus")
@Service(TopologyConstants.NODE_STATUS_SERVICE_NAME)
@TimeSeries(false)
@Partition({"cluster", "datacenter"})
@Indexes({
	@Index(name="Index_1_hostname", columns = { "hostname" }, unique = false)
	})
public class NodeStatusAPIEntity extends TaggedLogAPIEntity {

	@Column("a")
	private String nodeStatus;
	@Column("b")
	private String hadoopCronusVersion;
	@Column("c")
	private String systemCronusVersion;
	@Column("d")
	private String alertCronusVersion;
	@Column("e")
	private String cronusAgentStatus;
	@Column("f")
	private long lastModified;
	
	public String getNodeStatus() {
		return nodeStatus;
	}
	public void setNodeStatus(String nodeStatus) {
		this.nodeStatus = nodeStatus;
		valueChanged("nodeStatus");
	}
	public String getHadoopCronusVersion() {
		return hadoopCronusVersion;
	}
	public void setHadoopCronusVersion(String hadoopCronusVersion) {
		this.hadoopCronusVersion = hadoopCronusVersion;
		valueChanged("hadoopCronusVersion");
	}
	public String getSystemCronusVersion() {
		return systemCronusVersion;
	}
	public void setSystemCronusVersion(String systemCronusVersion) {
		this.systemCronusVersion = systemCronusVersion;
		valueChanged("systemCronusVersion");
	}
	public String getAlertCronusVersion() {
		return alertCronusVersion;
	}
	public void setAlertCronusVersion(String alertCronusVersion) {
		this.alertCronusVersion = alertCronusVersion;
		valueChanged("alertCronusVersion");
	}
	public String getCronusAgentStatus() {
		return cronusAgentStatus;
	}
	public void setCronusAgentStatus(String cronusAgentStatus) {
		this.cronusAgentStatus = cronusAgentStatus;
		valueChanged("cronusAgentStatus");
	}
	public long getLastModified() {
		return lastModified;
	}
	public void setLastModified(long lastModified) {
		this.lastModified = lastModified;
		valueChanged("lastModified");
	}
	

}
