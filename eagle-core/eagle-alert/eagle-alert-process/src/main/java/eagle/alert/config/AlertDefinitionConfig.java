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
package eagle.alert.config;

import java.io.Serializable;

public class AlertDefinitionConfig implements Serializable{

	private static final long serialVersionUID = 1L;
	private String id;
	private String description;
	private DeduplicatorConfig deduplicator;
	private NotificationConfig[] notifications;
	private Remediation[] remediations;	
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public NotificationConfig[] getNotifications() {
		return notifications;
	}
	public void setNotifications(NotificationConfig[] notifications) {
		this.notifications = notifications;
	}
	public Remediation[] getRemediations() {
		return remediations;
	}
	public void setRemediations(Remediation[] remediations) {
		this.remediations = remediations;
	}
	public DeduplicatorConfig getDeduplicator() {
		return deduplicator;
	}
	public void setDeduplicator(DeduplicatorConfig deduplicator) {
		this.deduplicator = deduplicator;
	}	
}
