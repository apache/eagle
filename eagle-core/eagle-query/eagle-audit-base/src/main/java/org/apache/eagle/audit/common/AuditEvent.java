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

package org.apache.eagle.audit.common;

import java.util.List;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;

public class AuditEvent extends java.util.EventObject {

	public AuditEvent(Object source, String serviceName, List<? extends TaggedLogAPIEntity> auditEntities) {
		super(source);
		this.serviceName = serviceName;
		this.auditEntities = auditEntities;
	}
	
	private String serviceName;
	private List<? extends TaggedLogAPIEntity> auditEntities;

	public String getServiceName() {
		return serviceName;
	}

	public List<? extends TaggedLogAPIEntity> getAuditEntities() {
		return auditEntities;
	}
	
	public String toString() {
        StringBuilder returnString = new StringBuilder(getClass().getName());
        returnString.append("[");
        returnString.append("serviceName=").append(getServiceName());
        returnString.append("; source=").append(getSource().getClass());
        returnString.append("]");
        return returnString.toString();
	}
}