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
package org.apache.eagle.dataproc.impl.analyze.entity;

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.entity.AbstractPolicyEntity;
import org.apache.eagle.log.entity.meta.Column;
import org.apache.eagle.log.entity.meta.ColumnFamily;
import org.apache.eagle.log.entity.meta.Index;
import org.apache.eagle.log.entity.meta.Indexes;
import org.apache.eagle.log.entity.meta.Prefix;
import org.apache.eagle.log.entity.meta.Service;
import org.apache.eagle.log.entity.meta.Table;
import org.apache.eagle.log.entity.meta.Tags;
import org.apache.eagle.log.entity.meta.TimeSeries;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Metaclass entity of stream analyze definition
 *
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("analyzedef")
@ColumnFamily("f")
@Prefix("analyzedef")
@Service(AlertConstants.ALERT_DEFINITION_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({"site", "dataSource", "analyzeExecutorId", "policyId", "policyType"})
@Indexes({
	@Index(name="Index_1_analyzeExecutorId", columns = { "analyzeExecutorID" }, unique = true),
})
@SuppressWarnings("serial")
public class AnalyzeDefinitionAPIEntity extends AbstractPolicyEntity {

	@Column("a")
	private String name;
	@Column("b")
	private String policyDef;
	@Column("c")
	private String description;
	@Column("d")
	private String notificationDef;
	@Column("e")
	private String remediationDef;
	@Column("f")
	private boolean enabled;
	@Column("g")
	private String owner;
	@Column("h")
	private long lastModifiedDate;
	@Column("j")
	private long createdTime;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPolicyDef() {
		return policyDef;
	}

	public void setPolicyDef(String analyzeDef) {
		this.policyDef = analyzeDef;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getNotificationDef() {
		return notificationDef;
	}

	public void setNotificationDef(String notificationDef) {
		this.notificationDef = notificationDef;
	}

	public String getRemediationDef() {
		return remediationDef;
	}

	public void setRemediationDef(String remediationDef) {
		this.remediationDef = remediationDef;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public long getLastModifiedDate() {
		return lastModifiedDate;
	}

	public void setLastModifiedDate(long lastModifiedDate) {
		this.lastModifiedDate = lastModifiedDate;
	}

	public long getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(long createdTime) {
		this.createdTime = createdTime;
	}

}
