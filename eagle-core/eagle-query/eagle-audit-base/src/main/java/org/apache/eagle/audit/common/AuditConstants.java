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

public class AuditConstants {

	public static final String AUDIT_SERVICE_ENDPOINT = "AuditService";
	
	// HBase Operations
	public static final String AUDIT_EVENT_CREATE = "CREATE";
	public static final String AUDIT_EVENT_UPDATE = "UPDATE";
	public static final String AUDIT_EVENT_DELETE = "DELETE";
	
	// Audit table details
	public static final String AUDIT_TABLE = "serviceAudit";
	public static final String AUDIT_COLUMN_SERVICE_NAME = "serviceName"; 
	public static final String AUDIT_COLUMN_USER_ID = "userID";
	public static final String AUDIT_COLUMN_OPERATION = "operation";
	public static final String AUDIT_COLUMN_TIMESTAMP = "auditTimestamp";
}