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

package org.apache.eagle.audit.listener;

import java.util.EventListenerProxy;

import org.apache.eagle.audit.common.AuditEvent;

public class AuditListenerProxy extends EventListenerProxy<AuditListener> implements AuditListener {

	private final String propertyName;
	
	public String getPropertyName() {
		return propertyName;
	}

	public AuditListenerProxy(String propertyName, AuditListener listener) {
		super(listener);
		this.propertyName = propertyName;
	}

	@Override
	public void auditEvent(AuditEvent event) {
		getListener().auditEvent(event);
	}
}