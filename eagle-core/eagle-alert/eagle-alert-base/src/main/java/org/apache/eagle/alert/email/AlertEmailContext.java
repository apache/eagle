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
package org.apache.eagle.alert.email;

import java.util.List;

/**
 * alert email bean
 * one email consists of a list of email component
 */
public class AlertEmailContext {
	private List<AlertEmailComponent> components;
	private String sender;
	private String subject;
	private String recipients;
	private String velocityTplFile;
	private String cc;
	
	public List<AlertEmailComponent> getComponents() {
		return components;
	}
	public void setComponents(List<AlertEmailComponent> components) {
		this.components = components;
	}
	public String getVelocityTplFile() {
		return velocityTplFile;
	}
	public void setVelocityTplFile(String velocityTplFile) {
		this.velocityTplFile = velocityTplFile;
	}
	public String getRecipients() {
		return recipients;
	}
	public void setRecipients(String recipients) {
		this.recipients = recipients;
	}
	public String getSender() {
		return sender;
	}
	public void setSender(String sender) {
		this.sender = sender;
	}
	public String getSubject() {
		return subject;
	}
	public void setSubject(String subject) {
		this.subject = subject;
	}
	public String getCc() {
		return cc;
	}
	public void setCc(String cc) {
		this.cc = cc;
	}
}
