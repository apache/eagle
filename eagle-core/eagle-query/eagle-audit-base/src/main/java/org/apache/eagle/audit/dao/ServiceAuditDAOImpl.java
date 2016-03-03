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

package org.apache.eagle.audit.dao;

import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.apache.eagle.audit.common.AuditConstants;
import org.apache.eagle.audit.entity.GenericAuditEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceAuditDAOImpl implements ServiceAuditDAO {
	
    private final Logger LOG = LoggerFactory.getLogger(ServiceAuditDAOImpl.class);
    private final EagleServiceConnector connector;
    
    public ServiceAuditDAOImpl(EagleServiceConnector connector){
        this.connector = connector;
    }

	@Override
    public List<GenericAuditEntity> findPolicyAudit(String site, String dataSource) throws Exception {
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(connector);
			String query = AuditConstants.AUDIT_SERVICE_ENDPOINT + "[@serviceName=\"AlertDefinitionService\" AND @site=\"" + site + "\" AND @dataSource=\"" + dataSource + "\"]{*}";
            GenericServiceAPIResponseEntity<GenericAuditEntity> response =  client.search().startTime(0).endTime(10 * DateUtils.MILLIS_PER_DAY).pageSize(Integer.MAX_VALUE).query(query).send();
            client.close();
            if (response.getException() != null) {
                throw new Exception("Exception in querying eagle service: " + response.getException());
            }
            return response.getObj();
		} catch (Exception exception) {
			LOG.error("Exception in retrieving audit entry: " + exception);
			throw new IllegalStateException(exception);
		}    	
    }

	@Override
    public List<GenericAuditEntity> findSiteAudit(String site) throws Exception {
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(connector);
			String query = AuditConstants.AUDIT_SERVICE_ENDPOINT + "[@serviceName=\"AlertDataSourceService\" AND @site=\"" + site + "\"]{*}";
            GenericServiceAPIResponseEntity<GenericAuditEntity> response =  client.search().startTime(0).endTime(10 * DateUtils.MILLIS_PER_DAY).pageSize(Integer.MAX_VALUE).query(query).send();
            client.close();
            if (response.getException() != null) {
                throw new Exception("Exception in querying eagle service: " + response.getException());
            }
            return response.getObj();
		} catch (Exception exception) {
			LOG.error("Exception in retrieving audit entry: " + exception);
			throw new IllegalStateException(exception);
		}    	
    }
    
	@Override
    public List<GenericAuditEntity> findDataSourceAudit(String dataSource) throws Exception {
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(connector);
			String query = AuditConstants.AUDIT_SERVICE_ENDPOINT + "[@serviceName=\"AlertDataSourceService\" AND @dataSource=\"" + dataSource + "\"]{*}";
            GenericServiceAPIResponseEntity<GenericAuditEntity> response =  client.search().startTime(0).endTime(10 * DateUtils.MILLIS_PER_DAY).pageSize(Integer.MAX_VALUE).query(query).send();
            client.close();
            if (response.getException() != null) {
                throw new Exception("Exception in querying eagle service: " + response.getException());
            }
            return response.getObj();
		} catch (Exception exception) {
			LOG.error("Exception in retrieving audit entry: " + exception);
			throw new IllegalStateException(exception);
		}  	
    }
    
	@Override
	public List<GenericAuditEntity> findServiceAudit(String serviceName) throws Exception {
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(connector);
			String query = AuditConstants.AUDIT_SERVICE_ENDPOINT + "[@serviceName=\"" + serviceName + "\"]{*}";
            GenericServiceAPIResponseEntity<GenericAuditEntity> response =  client.search().startTime(0).endTime(10 * DateUtils.MILLIS_PER_DAY).pageSize(Integer.MAX_VALUE).query(query).send();
            client.close();
            if (response.getException() != null) {
                throw new Exception("Exception in querying eagle service: " + response.getException());
            }
            return response.getObj();
		} catch (Exception exception) {
			LOG.error("Exception in retrieving audit entry: " + exception);
			throw new IllegalStateException(exception);
		}
	}

	@Override
	public List<GenericAuditEntity> findServiceAuditByUser(String serviceName, String userID) throws Exception {
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(connector);
			String query = AuditConstants.AUDIT_SERVICE_ENDPOINT + "[@serviceName=\"" + serviceName + "\" AND @userID=\"" + userID + "\"]{*}";
            GenericServiceAPIResponseEntity<GenericAuditEntity> response =  client.search().startTime(0).endTime(10 * DateUtils.MILLIS_PER_DAY).pageSize(Integer.MAX_VALUE).query(query).send();
            client.close();
            if (response.getException() != null) {
                throw new Exception("Exception in querying eagle service: " + response.getException());
            }
            return response.getObj();
		} catch (Exception exception) {
			LOG.error("Exception in retrieving audit entry: " + exception);
			throw new IllegalStateException(exception);
		}
	}

	@Override
	public List<GenericAuditEntity> findServiceAuditByAction(String serviceName, String action) throws Exception {
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(connector);
			String query = AuditConstants.AUDIT_SERVICE_ENDPOINT + "[@serviceName=\"" + serviceName + "\" AND @actionTaken=\"" + action + "\"]{*}";
            GenericServiceAPIResponseEntity<GenericAuditEntity> response =  client.search().startTime(0).endTime(10 * DateUtils.MILLIS_PER_DAY).pageSize(Integer.MAX_VALUE).query(query).send();
            client.close();
            if (response.getException() != null) {
                throw new Exception("Exception in querying eagle service: " + response.getException());
            }
            return response.getObj();
		} catch (Exception exception) {
			LOG.error("Exception in retrieving audit entry: " + exception);
			throw new IllegalStateException(exception);
		}
	}

	@Override
	public List<GenericAuditEntity> findUserServiceAudit(String userID) throws Exception {
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(connector);
			String query = AuditConstants.AUDIT_SERVICE_ENDPOINT + "[@userID=\"" + userID + "\"]{*}";
            GenericServiceAPIResponseEntity<GenericAuditEntity> response =  client.search().startTime(0).endTime(10 * DateUtils.MILLIS_PER_DAY).pageSize(Integer.MAX_VALUE).query(query).send();
            client.close();
            if (response.getException() != null) {
                throw new Exception("Exception in querying eagle service: " + response.getException());
            }
            return response.getObj();
		} catch (Exception exception) {
			LOG.error("Exception in retrieving audit entry: " + exception);
			throw new IllegalStateException(exception);
		}
	}

	@Override
	public List<GenericAuditEntity> findUserServiceAuditByAction(String userID, String action) throws Exception {
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(connector);
			String query = AuditConstants.AUDIT_SERVICE_ENDPOINT + "[@userID=\"" + userID + "\" AND @actionTaken=\"" + action + "\"]{*}";
            GenericServiceAPIResponseEntity<GenericAuditEntity> response =  client.search().startTime(0).endTime(10 * DateUtils.MILLIS_PER_DAY).pageSize(Integer.MAX_VALUE).query(query).send();
            client.close();
            if (response.getException() != null) {
                throw new Exception("Exception in querying eagle service: " + response.getException());
            }
            return response.getObj();
		} catch (Exception exception) {
			LOG.error("Exception in retrieving audit entry: " + exception);
			throw new IllegalStateException(exception);
		}
	}
}