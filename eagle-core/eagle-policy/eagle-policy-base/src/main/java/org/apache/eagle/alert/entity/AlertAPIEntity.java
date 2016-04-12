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

import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.common.metric.AlertContext;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table("alertdetail")
@ColumnFamily("f")
@Prefix("hadoop")
@Service(Constants.ALERT_SERVICE_ENDPOINT_NAME)
@TimeSeries(true)
@JsonIgnoreProperties(ignoreUnknown = true)
@Tags({
		"site",
		"hostname",
		"application",
		"policyId",
		"sourceStreams",
		"alertSource",
		"alertExecutorId"
})
public class AlertAPIEntity extends TaggedLogAPIEntity{
	@Column("description")
	private String description;
	@Column("remediationID")
	private String remediationID;
	@Column("remediationCallback")
	private String remediationCallback;
	@Column("alertContext")
	private String alertContext;
	@Column("streamId")
	private String streamId;

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

	public String getAlertContext() {
		return alertContext;
	}

	public AlertContext getWrappedAlertContext() {
		return AlertContext.fromJsonString(alertContext);
	}
	
	public void setAlertContext(String alertContext) {
		this.alertContext = alertContext;
		_pcs.firePropertyChange("alertContext", null, null);
	}

	public void setDecodedAlertContext(AlertContext alertContext) {
		if(alertContext != null) this.alertContext = alertContext.toJsonString();
		_pcs.firePropertyChange("alertContext", null, null);
	}

	public String getStreamId() {
		return streamId;
	}

	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}
}