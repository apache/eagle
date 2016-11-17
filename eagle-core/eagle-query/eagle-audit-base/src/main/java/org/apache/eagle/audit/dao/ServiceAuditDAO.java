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

import org.apache.eagle.audit.entity.GenericAuditEntity;

import java.util.List;

public interface ServiceAuditDAO {

    /**
     * Retrieve all audits of alert definition for a specific site and data source.
     *
     * @return
     * @throws Exception
     */
    List<GenericAuditEntity> findPolicyAudit(String site, String dataSource) throws Exception;

    /**
     * Retrieve all audits of site definition for the given site.
     *
     * @return
     * @throws Exception
     */
    List<GenericAuditEntity> findSiteAudit(String site) throws Exception;

    /**
     * Retrieve all audits of datasource definition for the given data source.
     *
     * @param dataSource
     * @return
     * @throws Exception
     */
    List<GenericAuditEntity> findDataSourceAudit(String dataSource) throws Exception;

    /**
     * Retrieve all audits specific to a service.
     *
     * @param serviceName
     * @return
     * @throws Exception
     */
    List<GenericAuditEntity> findServiceAudit(String serviceName) throws Exception;

    /**
     * Retrieve all audits specific to a service and specific to a userID.
     *
     * @param serviceName
     * @param userID
     * @return
     * @throws Exception
     */
    List<GenericAuditEntity> findServiceAuditByUser(String serviceName, String userID) throws Exception;

    /**
     * Retrieve all audits specific to a service and specific to an action.
     *
     * @param serviceName
     * @param action
     * @return
     * @throws Exception
     */
    List<GenericAuditEntity> findServiceAuditByAction(String serviceName, String action) throws Exception;

    /**
     * Retrieve all audits specific to a user.
     *
     * @param userID
     * @return
     * @throws Exception
     */
    List<GenericAuditEntity> findUserServiceAudit(String userID) throws Exception;

    /**
     * Retrieve all audits specific to a user and specific to an action.
     *
     * @param userID
     * @param action
     * @return
     * @throws Exception
     */
    List<GenericAuditEntity> findUserServiceAuditByAction(String userID, String action) throws Exception;
}