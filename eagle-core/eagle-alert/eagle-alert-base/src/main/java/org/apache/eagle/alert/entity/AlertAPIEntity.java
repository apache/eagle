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
import org.apache.eagle.common.metric.AlertContext;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.Column;
import org.apache.eagle.log.entity.meta.ColumnFamily;
import org.apache.eagle.log.entity.meta.Prefix;
import org.apache.eagle.log.entity.meta.Service;
import org.apache.eagle.log.entity.meta.Table;
import org.apache.eagle.log.entity.meta.TimeSeries;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("alertdetail")
@ColumnFamily("f")
@Prefix("hadoop")
@Service(AlertConstants.ALERT_SERVICE_ENDPOINT_NAME)
@TimeSeries(true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertAPIEntity extends TaggedLogAPIEntity{
	@Column("description")
	private String description;
	@Column("remediationID")
	private String remediationID;
	@Column("remediationCallback")
	private String remediationCallback;
	@Column("alertContext")
	private AlertContext alertContext;

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
		_pcs.firePropertyChange("description", null, null);
	}

	public String getRemediationID() {
		return remediationID;
	}

	public void setRemediationID(String remediationID) {
		this.remediationID = remediationID;
		_pcs.firePropertyChange("remediationID", null, null);
	}

	public String getRemediationCallback() {
		return remediationCallback;
	}

	public void setRemediationCallback(String remediationCallback) {
		this.remediationCallback = remediationCallback;
		_pcs.firePropertyChange("remediationCallback", null, null);
	}

	public AlertContext getAlertContext() {
		return alertContext;
	}
	
	public void setAlertContext(AlertContext alertContext) {
		this.alertContext = alertContext;
		_pcs.firePropertyChange("alertContext", null, null);
	}
}