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
import org.apache.eagle.service.application.dao.ApplicationManagerDAO;
import org.apache.eagle.service.application.dao.ApplicationManagerDaoImpl;
import org.apache.eagle.service.application.entity.TopologyExecutionEntity;
import org.apache.eagle.service.application.entity.TopologyOperationEntity;
import org.apache.eagle.stream.application.model.TopologyOperationModel;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
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
                TopologyOperationModel operation = TopologyOperationEntity.toModel(entity);
                String status = dao.loadTopologyExecutionStatus(operation.site(), operation.application(), operation.topology());
                if(status == null) {
                    throw new Exception(String.format("Fail to fetch the topology execution status by site=%s, application=%s, topology=%s", operation.site(), operation.application(), operation.topology()));
                }
                int operationsInRunning = dao.loadTopologyOperationInRunning(operation.site(), operation.application(), operation.topology());
                if(operationsInRunning !=0) {
                    throw new Exception(operationsInRunning + "operations are running, please wait for a minute");
                }
                if (validateOperation(operation.operation(), status)) {
                    Map<String, String> tags = entity.getTags();
                    tags.put("uuid", UUID.randomUUID().toString());
                    entity.setTags(tags);
                    entity.setLastModifiedDate(System.currentTimeMillis());
                    operations.add(entity);
                } else {
                    throw new Exception(String.format("%s is an invalid operation, as the topology's current status is %s", operation.operation(), status));
                }
            }
            dao.createOperation(operations);
        } catch (Exception e) {
            response.setSuccess(false);
            response.setException(e);
        }
        return response;
    }

    private boolean validateOperation(String operation, String status) {
        boolean ret = true;
        switch (operation) {
            case TopologyOperationEntity.OPERATION.START:
                if(status == TopologyExecutionEntity.TOPOLOGY_STATUS.STARTED || status == TopologyExecutionEntity.TOPOLOGY_STATUS.PENDING)
                    ret = false;
                break;
            case TopologyOperationEntity.OPERATION.STOP:
                if(status != TopologyExecutionEntity.TOPOLOGY_STATUS.STARTED)
                    ret = false;
                break;
            default: break;
        }
        return ret;
    }

    private List<? extends TaggedLogAPIEntity> unmarshalOperationEntities(InputStream inputStream) throws IllegalAccessException, InstantiationException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(inputStream, TypeFactory.defaultInstance().constructCollectionType(LinkedList.class, TopologyOperationEntity.class));
    }
}
