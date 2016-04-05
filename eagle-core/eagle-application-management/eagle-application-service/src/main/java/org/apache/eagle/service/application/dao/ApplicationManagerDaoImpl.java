/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.service.application.dao;


import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.service.application.entity.TopologyExecutionEntity;
import org.apache.eagle.service.application.entity.TopologyOperationEntity;
import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ApplicationManagerDaoImpl implements ApplicationManagerDAO {
    private static Logger LOG = LoggerFactory.getLogger(ApplicationManagerDaoImpl.class);
    GenericEntityServiceResource resource = new GenericEntityServiceResource();

    @Override
    public String loadTopologyExecutionStatus(String site, String application, String topology) {
        String query = String.format("%s[@site=\"%s\" AND @application=\"%s\" AND @topology=\"%s\"]{*}", Constants.TOPOLOGY_EXECUTION_SERVICE_ENDPOINT_NAME, site, application, topology);
        GenericServiceAPIResponseEntity<TopologyExecutionEntity> response = resource.search(query,  null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false, 0, null, false);
        if(!response.isSuccess()) {
            LOG.error(response.getException());
            return null;
        }
        List<TopologyExecutionEntity> list = response.getObj();
        if(list == null || list.size() != 1) {
            LOG.error("ERROR: fetching 0 or more than 1 topology execution entities");
            return null;
        }
        return list.get(0).getStatus();
    }

    @Override
    public int loadTopologyOperationInRunning(String site, String application, String topology) throws Exception {
        int ret = 0;
        String query = String.format("%s[@site=\"%s\" AND @application=\"%s\" AND @topology=\"%s\" AND (@status=\"%s\" OR @status=\"%s\")]{*}", Constants.TOPOLOGY_OPERATION_SERVICE_ENDPOINT_NAME, site, application, topology, TopologyOperationEntity.OPERATION_STATUS.INITIALIZED, TopologyOperationEntity.OPERATION_STATUS.PENDING);
        GenericServiceAPIResponseEntity<TopologyExecutionEntity> response = resource.search(query, null, null, Integer.MAX_VALUE, null, false, false, 0L, 0, false, 0, null, false);
        if(!response.isSuccess()) {
            throw new Exception(response.getException());
        }
        if(response.getObj() != null && response.getObj().size() != 0) {
            ret = response.getObj().size();
        }
        return ret;
    }

    @Override
    public void createOperation(List<TopologyOperationEntity> entities) throws Exception {
        if(entities.size() == 0) {
            LOG.info("TopologyOperationEntity set is empty.");
        }
        GenericServiceAPIResponseEntity response = resource.updateEntities(entities, Constants.TOPOLOGY_OPERATION_SERVICE_ENDPOINT_NAME);
        if(!response.isSuccess()) {
            throw new Exception(response.getException());
        }
    }
}

