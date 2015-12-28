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
package org.apache.eagle.dataproc.impl.aggregate.entity;

import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.policy.entity.AbstractPolicyDefinitionEntity;
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
 * entity of stream analyze definition
 *
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("analyzedef")
@ColumnFamily("f")
@Prefix("analyzedef")
@Service(Constants.ALERT_DEFINITION_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({"site", "dataSource", "analyzeExecutorId", "policyId", "policyType"})
@Indexes({
	@Index(name="Index_1_analyzeExecutorId", columns = { "analyzeExecutorID" }, unique = true),
})
@SuppressWarnings("serial")
public class AggregateDefinitionAPIEntity extends AbstractPolicyDefinitionEntity {

	@Column("a")
	private String name;
	@Column("b")
	private String policyDef;
	@Column("c")
	private String description;
	@Column("d")
	private boolean enabled;
	@Column("e")
	private String owner;
	@Column("f")
	private long lastModifiedDate;
	@Column("g")
	private long createdTime;
	/**
	 * TODO: By setting this to true, eagle would generate a simple sampling for each field given.
	 * for each field, an
	 * 	avg(Field)
	 * 	min(Field)
	 * 	max(Field)
	 * 	count(Field) would be collected
	 */
	@Column("h")
	private boolean downSampling;

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

	public boolean isDownSampling() {
		return downSampling;
	}

	public void setDownSampling(boolean downSampling) {
		this.downSampling = downSampling;
	}

}
