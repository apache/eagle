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

package org.apache.eagle.audit.entity;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.ColumnFamily;
import org.apache.eagle.log.entity.meta.Prefix;
import org.apache.eagle.log.entity.meta.Service;
import org.apache.eagle.log.entity.meta.Table;
import org.apache.eagle.log.entity.meta.Tags;
import org.apache.eagle.log.entity.meta.TimeSeries;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Objects;

import static org.apache.eagle.audit.common.AuditConstants.AUDIT_COLUMN_OPERATION;
import static org.apache.eagle.audit.common.AuditConstants.AUDIT_COLUMN_SERVICE_NAME;
import static org.apache.eagle.audit.common.AuditConstants.AUDIT_COLUMN_TIMESTAMP;
import static org.apache.eagle.audit.common.AuditConstants.AUDIT_COLUMN_USER_ID;
import static org.apache.eagle.audit.common.AuditConstants.AUDIT_SERVICE_ENDPOINT;
import static org.apache.eagle.audit.common.AuditConstants.AUDIT_TABLE;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@Table(AUDIT_TABLE)
@ColumnFamily("f")
@Prefix(AUDIT_TABLE)
@Service(AUDIT_SERVICE_ENDPOINT)
@JsonIgnoreProperties(ignoreUnknown = true)
@TimeSeries(false)
@Tags({AUDIT_COLUMN_SERVICE_NAME, AUDIT_COLUMN_USER_ID, AUDIT_COLUMN_OPERATION, AUDIT_COLUMN_TIMESTAMP})
public class GenericAuditEntity extends TaggedLogAPIEntity {
	
	public boolean equals (Object obj) {
		if(obj == this)
			return true;
		if(!(obj instanceof GenericAuditEntity))
			return false;
		GenericAuditEntity that = (GenericAuditEntity) obj;
		return compare(that.getTags().get(AUDIT_COLUMN_SERVICE_NAME), this.getTags().get(AUDIT_COLUMN_SERVICE_NAME)) &&
					 compare(that.getTags().get(AUDIT_COLUMN_USER_ID), this.getTags().get(AUDIT_COLUMN_USER_ID)) &&
					 compare(that.getTags().get(AUDIT_COLUMN_OPERATION), this.getTags().get(AUDIT_COLUMN_OPERATION)) &&
					 compare(that.getTags().get(AUDIT_COLUMN_TIMESTAMP), this.getTags().get(AUDIT_COLUMN_TIMESTAMP));
	}
	
	private boolean compare(String a, String b){
		if(Objects.equals(a, b))
			return true;
		if(a == null || b == null)
			return false;
		if(a.equals(b))
			return true;
		return false;
	}

	public int hashCode(){
		HashCodeBuilder builder = new HashCodeBuilder();
		builder.append(getTags().get(AUDIT_COLUMN_SERVICE_NAME));
		builder.append(getTags().get(AUDIT_COLUMN_USER_ID));
		builder.append(getTags().get(AUDIT_COLUMN_OPERATION));
		builder.append(getTags().get(AUDIT_COLUMN_TIMESTAMP));
		return builder.toHashCode();
	}
}