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
package org.apache.eagle.alert.entity;

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.Column;
import org.apache.eagle.log.entity.meta.ColumnFamily;
import org.apache.eagle.log.entity.meta.Index;
import org.apache.eagle.log.entity.meta.Indexes;
import org.apache.eagle.log.entity.meta.Prefix;
import org.apache.eagle.log.entity.meta.Service;
import org.apache.eagle.log.entity.meta.Table;
import org.apache.eagle.log.entity.meta.Tags;
import org.apache.eagle.log.entity.meta.TimeSeries;

/**
 * site: site name
 * dataSource: data source name
 *
 * alertExecutorId: Group Policy by alertExecutorId, the policy definition with the sample ["site", "dataSource", "alertExecutorId"] should run on the sample alert executor
 *
 * policyId: policy name, should be unique
 * policyType: policy engine implementation type
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("alertdef")
@ColumnFamily("f")
@Prefix("alertdef")
@Service(AlertConstants.ALERT_DEFINITION_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({"site", "dataSource", "alertExecutorId", "policyId", "policyType"})
@Indexes({
	@Index(name="Index_1_alertExecutorId", columns = { "alertExecutorID" }, unique = true),
})
public class AlertDefinitionAPIEntity extends TaggedLogAPIEntity{
	@Column("a")
	private String desc;
	@Column("b")
	private String policyDef;
	@Column("c")
	private String dedupeDef;
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
	@Column("i")
	private long severity;
	@Column("j")
	private long createdTime;

	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
		valueChanged("desc");
	}
	public String getPolicyDef() {
		return policyDef;
	}
	public void setPolicyDef(String policyDef) {
		this.policyDef = policyDef;
		valueChanged("policyDef");
	}
	public String getDedupeDef() {
		return dedupeDef;
	}
	public void setDedupeDef(String dedupeDef) {
		this.dedupeDef = dedupeDef;
		valueChanged("dedupeDef");
	}
	public String getNotificationDef() {
		return notificationDef;
	}
	public void setNotificationDef(String notificationDef) {
		this.notificationDef = notificationDef;
		valueChanged("notificationDef");
	}
	public String getRemediationDef() {
		return remediationDef;
	}
	public void setRemediationDef(String remediationDef) {
		this.remediationDef = remediationDef;
		valueChanged("remediationDef");
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
	public long getSeverity() {
		return severity;
	}
	public void setSeverity(long severity) {
		this.severity = severity;
		valueChanged("severity");
	}	
	public long getCreatedTime() {
		return createdTime;
	}
	public void setCreatedTime(long createdTime) {
		this.createdTime = createdTime;
		valueChanged("createdTime");
	}
	public boolean equals(Object o){
		if(o == this)
			return true;
		if(!(o instanceof AlertDefinitionAPIEntity))
			return false;
		AlertDefinitionAPIEntity that = (AlertDefinitionAPIEntity)o;
		if(that.enabled == this.enabled &&
				compare(that.policyDef, this.policyDef) &&
				compare(that.dedupeDef, this.dedupeDef) &&
				compare(that.notificationDef, this.notificationDef) &&
				compare(that.remediationDef, this.remediationDef))
			return true;
		return false;
	}
	
	private boolean compare(String a, String b){
		if(a == b)
			return true;
		if(a == null || b == null)
			return false;
		if(a.equals(b))
			return true;
		return false;
	}
	
	public int hashCode(){
		HashCodeBuilder builder = new HashCodeBuilder();
		builder.append(enabled);
		builder.append(policyDef);
		builder.append(dedupeDef);
		builder.append(notificationDef);
		builder.append(remediationDef);
		return builder.toHashCode();
	}
}
