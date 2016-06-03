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

package org.apache.eagle.service.application;


import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.service.application.dao.ApplicationManagerDAO;
import org.apache.eagle.service.application.dao.ApplicationManagerDaoImpl;
import org.apache.eagle.service.application.entity.TopologyExecutionStatus;
import org.apache.eagle.service.application.entity.TopologyOperationEntity;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Path(ApplicationManagementResource.ROOT_PATH)
public class ApplicationManagementResource {
    private final static ApplicationManagerDAO dao = new ApplicationManagerDaoImpl();
    public final static String ROOT_PATH = "/app";

    @Path("operation")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity createOperation(InputStream inputStream) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity<>();
        List<TopologyOperationEntity> operations = new LinkedList<>();
        try {
            List<TopologyOperationEntity> entities = (List<TopologyOperationEntity>) unmarshalOperationEntities(inputStream);
            if (entities == null) {
                throw new IllegalArgumentException("inputStream cannot convert to TopologyOperationEntity");
            }
            for (TopologyOperationEntity entity : entities) {
                String status = dao.loadTopologyExecutionStatus(entity.getSite(), entity.getApplication(), entity.getTopology());
                if(status == null) {
                    throw new Exception(String.format("Fail to fetch the topology execution status by site=%s, application=%s, topology=%s", entity.getSite(), entity.getApplication(), entity.getTopology()));
                }
                int operationsInRunning = dao.loadTopologyOperationsInRunning(entity.getSite(), entity.getApplication(), entity.getTopology());
                if(operationsInRunning !=0) {
                    throw new Exception(operationsInRunning + "operations are running, please wait for a minute");
                }
                if (validateOperation(entity.getOperation(), status)) {
                    Map<String, String> tags = entity.getTags();
                    tags.put(AppManagerConstants.OPERATION_ID_TAG, UUID.randomUUID().toString());
                    entity.setTags(tags);
                    entity.setLastModifiedDate(System.currentTimeMillis());
                    entity.setTimestamp(System.currentTimeMillis());
                    operations.add(entity);
                } else {
                    throw new Exception(String.format("%s is an invalid operation, as the topology's current status is %s", entity.getOperation(), status));
                }
            }
            response = dao.createOperation(operations);
        } catch (Exception e) {
            response.setSuccess(false);
            response.setException(e);
        }
        return response;
    }

    private boolean validateOperation(String operation, String status) {
        boolean ret = false;
        switch (operation) {
            case TopologyOperationEntity.OPERATION.START:
                return TopologyExecutionStatus.isReadyToStart(status);
            case TopologyOperationEntity.OPERATION.STOP:
                return TopologyExecutionStatus.isReadyToStop(status);
            default: break;
        }
        return ret;
    }

    private List<? extends TaggedLogAPIEntity> unmarshalOperationEntities(InputStream inputStream) throws IllegalAccessException, InstantiationException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(inputStream, TypeFactory.defaultInstance().constructCollectionType(LinkedList.class, TopologyOperationEntity.class));
    }

    @Path("topology")
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity deleteTopology(@QueryParam("topology") String topology) {
        return dao.deleteTopology(topology);
    }


}
