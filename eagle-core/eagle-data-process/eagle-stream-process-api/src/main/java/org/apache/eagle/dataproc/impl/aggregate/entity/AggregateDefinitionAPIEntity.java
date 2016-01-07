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

import org.apache.eagle.alert.entity.AbstractPolicyDefinitionEntity;
import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.policy.common.Constants;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * entity of stream analyze definition
 *
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("aggregatedef")
@ColumnFamily("f")
@Prefix("aggregatedef")
@Service(Constants.AGGREGATE_DEFINITION_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({"site", "dataSource", "executorId", "policyId", "policyType"})
@Indexes({
	@Index(name="Index_1_aggregateExecutorId", columns = { "executorId" }, unique = true),
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

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPolicyDef() {
		return policyDef;
	}

	public void setPolicyDef(String policyDef) {
		this.policyDef = policyDef;
		valueChanged("policyDef");
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
		valueChanged("description");
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
		valueChanged("enabled");
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
		valueChanged("owner");
	}

	public long getLastModifiedDate() {
		return lastModifiedDate;
	}

	public void setLastModifiedDate(long lastModifiedDate) {
		this.lastModifiedDate = lastModifiedDate;
		valueChanged("lastModifiedDate");
	}

	public long getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(long createdTime) {
		this.createdTime = createdTime;
		valueChanged("createdTime");
	}


}
