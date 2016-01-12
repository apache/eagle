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

package org.apache.eagle.storage.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.eagle.audit.common.AuditEvent;
import org.apache.eagle.audit.entity.GenericAuditEntity;
import org.apache.eagle.audit.listener.AuditListener;
import org.apache.eagle.audit.listener.AuditSupport;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.storage.DataStorageManager;
import org.apache.eagle.storage.exception.IllegalDataStorageTypeException;
import org.apache.eagle.storage.operation.CreateStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import static org.apache.eagle.audit.common.AuditConstants.AUDIT_SERVICE_ENDPOINT;
import static org.apache.eagle.audit.common.AuditConstants.AUDIT_COLUMN_SERVICE_NAME;
import static org.apache.eagle.audit.common.AuditConstants.AUDIT_COLUMN_USER_ID;
import static org.apache.eagle.audit.common.AuditConstants.AUDIT_COLUMN_OPERATION;
import static org.apache.eagle.audit.common.AuditConstants.AUDIT_COLUMN_TIMESTAMP;

/**
 * Implementation of AuditListener class. 
 * Used in HBaseStorage class for auditing HBase operations performed.
 */
public class HBaseStorageAudit implements AuditListener {
	
	private final static Logger LOG = LoggerFactory.getLogger(HBaseStorageAudit.class);
	private AuditSupport auditSupport = new AuditSupport(this);
	
	public HBaseStorageAudit() {
		auditSupport.addAuditListener(this);
	}
	
	@Override
	public void auditEvent(AuditEvent event) {
			LOG.info("firing audit event: " + event.toString());
			persistAuditEntity(event.getAuditEntities());
	}
	
	/**
	 * Method to be invoked for firing audit event.
	 * @param operation: HBase operation. Values like CREATE/UPDATE/DELETE.
	 * @param entities: List of entities used in HBase operation.
	 * @param encodedRowKeys: List of encodededRowKeys returned from successful HBase operation. To be passed only from deletebyID method. 
	 * @param entityDefinition: EntityDefinition object used in the HBaseOperation.
	 */
    public void auditOperation(String operation, List<? extends TaggedLogAPIEntity> entities, List<String> encodedRowKeys, EntityDefinition entityDefinition) {
    	if (isAuditingRequired(entityDefinition.getService())) {
    		List<GenericAuditEntity> auditEntities = buildAuditEntities(operation, entities, encodedRowKeys, entityDefinition);
    		if (null != auditEntities && 0 != auditEntities.size())
    			auditSupport.fireAudit(entityDefinition.getService(), auditEntities);
    	}
    }
    
    /**
     * Check if audit is required based on the service names and audit configuration.
     * @param serviceName: Name of the service call.
     * @return
     */
    private boolean isAuditingRequired (String serviceName) {
    	if (EagleConfigFactory.load().isServiceAuditingEnabled()
    			// As per jira EAGLE-47, HBase operation level audit is done only for Policy, Site and DataSource definitions. 
    			&& ("AlertDefinitionService".equals(serviceName) || "AlertDataSourceService".equals(serviceName))) {
    		return true;
    	}

    	return false;
    }
    
    /**
     * Build Audit entities based on the available infomration.
     * @param operation: HBase operation performed.
     * @param entities: List of entities used in HBase operation.
     * @param encodedRowKeys: List of encodededRowKeys returned from successful HBase operation. To be passed only from deletebyID method.
     * @param entityDefinition: EntityDefinition object used in the HBaseOperation.
     * @return
     */
    private List<GenericAuditEntity> buildAuditEntities(String operation, List<? extends TaggedLogAPIEntity> entities, List<String> encodedRowKeys, EntityDefinition entityDefinition) {
    	String serviceName = entityDefinition.getService();
    	long timestamp = System.currentTimeMillis()/1000L;
    	
    	Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    	String userID = null != authentication.getName() ? authentication.getName() : "data not available"; // empty user

    	List<GenericAuditEntity> auditEntities = new ArrayList<GenericAuditEntity>();
    	GenericAuditEntity auditEntity = new GenericAuditEntity();
    	
		if (null != entities && 0 != entities.size())  {
			Map<String, String> auditTags;
			for (TaggedLogAPIEntity entity : entities) {
	    		auditTags = entity.getTags();
	    		auditTags.put(AUDIT_COLUMN_SERVICE_NAME, serviceName);
	    		auditTags.put(AUDIT_COLUMN_USER_ID, userID);
	    		auditTags.put(AUDIT_COLUMN_OPERATION, operation);
	    		auditTags.put(AUDIT_COLUMN_TIMESTAMP, timestamp + "");
	    		
	    		auditEntity = new GenericAuditEntity();
	    		auditEntity.setTags(auditTags);
	    		auditEntities.add(auditEntity);
	    	}
			
			return auditEntities;
		} else if (null != encodedRowKeys && 0 != encodedRowKeys.size()) { // conditions yields true only in case of deleteByID 
			Map<String, String> auditTags;
			for (String encodedRowKey : encodedRowKeys) {
				auditTags = new HashMap<String, String>();
				auditTags.put("encodedRowKey", encodedRowKey);
				auditTags.put(AUDIT_COLUMN_SERVICE_NAME, serviceName);
	    		auditTags.put(AUDIT_COLUMN_USER_ID, userID);
	    		auditTags.put(AUDIT_COLUMN_OPERATION, operation);
	    		auditTags.put(AUDIT_COLUMN_TIMESTAMP, timestamp + "");
	    		
	    		auditEntity = new GenericAuditEntity();
	    		auditEntity.setTags(auditTags);
	    		auditEntities.add(auditEntity);
			}

			return auditEntities;
		} else {
			return null;
		}
    }

    /**
     * Persists audit entries into HBase.
     * @param entityList
     */
    private void persistAuditEntity(List<? extends TaggedLogAPIEntity> entityList) {
    	try {
	    	if (null != entityList && 0 != entityList.size()) {
		    	CreateStatement createStatement = new CreateStatement(entityList, AUDIT_SERVICE_ENDPOINT);
		        createStatement.execute(DataStorageManager.newDataStorage("hbase"));
	    	}
    	} catch (IOException | IllegalDataStorageTypeException exception) {
			LOG.error("exception in auditing storage event", exception.getMessage());
		}
    }
}