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
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.Column;
import org.apache.eagle.log.entity.meta.ColumnFamily;
import org.apache.eagle.log.entity.meta.Prefix;
import org.apache.eagle.log.entity.meta.Service;
import org.apache.eagle.log.entity.meta.Table;
import org.apache.eagle.log.entity.meta.Tags;
import org.apache.eagle.log.entity.meta.TimeSeries;

/**
 * ddl to create streammetadata table
 * 
 * create 'alertStreamSchema', {NAME => 'f', BLOOMFILTER => 'ROW', VERSIONS => '1', COMPRESSION => 'SNAPPY'}
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("alertStreamSchema")
@ColumnFamily("f")
@Prefix("alertStreamSchema")
@Service(AlertConstants.ALERT_STREAM_SCHEMA_SERVICE_ENDPOINT_NAME)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({"dataSource", "streamName", "attrName"})
public class AlertStreamSchemaEntity extends TaggedLogAPIEntity{
	@Column("a")
	private String attrType;
	@Column("b")
	private String category;
	@Column("c")
	private String attrValueResolver;
	/* all tags form the key for alert de-duplication */
	@Column("d")
	private Boolean usedAsTag;
	@Column("e")
	private String attrDescription;
	@Column("f")
	private String attrDisplayName;	
	@Column("g")
	private String defaultValue;

	public String getAttrType() {
		return attrType;
	}
	public void setAttrType(String attrType) {
		this.attrType = attrType;
		valueChanged("attrType");
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
		valueChanged("category");
	}
	public String getAttrValueResolver() {
		return attrValueResolver;
	}
	public void setAttrValueResolver(String attrValueResolver) {
		this.attrValueResolver = attrValueResolver;
		valueChanged("attrValueResolver");
	}
	public Boolean getUsedAsTag() {
		return usedAsTag;
	}
	public void setUsedAsTag(Boolean usedAsTag) {
		this.usedAsTag = usedAsTag;
		valueChanged("usedAsTag");
	}
	public String getAttrDescription() {
		return attrDescription;
	}
	public void setAttrDescription(String attrDescription) {
		this.attrDescription = attrDescription;
		valueChanged("attrDescription");
	}
	public String getAttrDisplayName() {
		return attrDisplayName;
	}
	public void setAttrDisplayName(String attrDisplayName) {
		this.attrDisplayName = attrDisplayName;
		valueChanged("attrDisplayName");
	}
	public String getDefaultValue() {
		return defaultValue;
	}
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
		valueChanged("defaultValue");
	}
}