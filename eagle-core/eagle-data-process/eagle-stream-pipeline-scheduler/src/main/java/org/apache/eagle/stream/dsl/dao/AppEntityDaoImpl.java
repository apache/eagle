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

package org.apache.eagle.stream.dsl.dao;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;


public class AppEntityDaoImpl implements AppEntityDao {
    private static final Logger LOG = LoggerFactory.getLogger(AppEntityDaoImpl.class);

    private final String eagleServiceHost;
    private final Integer eagleServicePort;
    private String username;
    private String password;

    public AppEntityDaoImpl(String eagleServiceHost, int eagleServicePort, String username, String password) {
        this.eagleServiceHost = eagleServiceHost;
        this.eagleServicePort = eagleServicePort;
        this.username = username;
        this.password = password;
    }

    public AppEntityDaoImpl(String eagleServiceHost, int eagleServicePort) {
        this.eagleServiceHost = eagleServiceHost;
        this.eagleServicePort = eagleServicePort;
        this.username = null;
        this.password = null;
    }

    @Override
    public GenericServiceAPIResponseEntity create(List<? extends TaggedLogAPIEntity> entities, String serviceName) throws Exception {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();
        try {
            int total = entities.size();
            EagleServiceClientImpl client = new EagleServiceClientImpl(eagleServiceHost, eagleServicePort, username, password);
            response = client.create(entities, serviceName);
            if(response.isSuccess()) {
                LOG.info("Wrote " + total + " entities to service " + serviceName);
            }else{
                LOG.error("Failed to write " + total + " entities to service, due to server exception: "+ response.getException());
            }
            client.close();
        }
        catch (Exception ex) {
            LOG.error("Got exception while writing entities: ", ex);
            response.setException(ex);
        }
        return response;
    }

    @Override
    public GenericServiceAPIResponseEntity update(TaggedLogAPIEntity entity, String serviceName) throws Exception {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();
        try {
            EagleServiceClientImpl client = new EagleServiceClientImpl(eagleServiceHost, eagleServicePort, username, password);
            response = client.update(Arrays.asList(entity), serviceName);
            if(response.isSuccess()) {
                LOG.info("Updated a entity to service " + serviceName);
            }else{
                LOG.error("Failed to update a entity to service, due to server exception: "+ response.getException());
            }
            client.close();
        }
        catch (Exception ex) {
            LOG.error("Got exception while updating entities: ", ex);
            response.setException(ex);
        }
        return response;
    }

    @Override
    public GenericServiceAPIResponseEntity deleteByEntities(List<? extends TaggedLogAPIEntity> entities, String serviceName) throws Exception {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();
        try {
            int total = entities.size();
            EagleServiceClientImpl client = new EagleServiceClientImpl(eagleServiceHost, eagleServicePort, username, password);
            response = client.delete(entities, serviceName);
            if(response.isSuccess()) {
                LOG.info("Deleted " + total + " entities");
            }else{
                LOG.error("Failed to delete " + total + " entities, due to server exception: "+ response.getException());
            }
            client.close();
        }
        catch (Exception ex) {
            LOG.error("Got exception while writing entities: ", ex);
            response.setException(ex);
        }
        return response;
    }

    @Override
    public GenericServiceAPIResponseEntity deleteByIds(List<String> ids, String serviceName) throws Exception {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();
        try {
            int total = ids.size();
            EagleServiceClientImpl client = new EagleServiceClientImpl(eagleServiceHost, eagleServicePort, username, password);
            response = client.deleteById(ids, serviceName);
            if(response.isSuccess()) {
                LOG.info("Deleted " + total + " entities by Ids");
            }else{
                LOG.error("Failed to delete " + total + " entities, due to server exception: "+ response.getException());
            }
            client.close();
        }
        catch (Exception ex) {
            LOG.error("Got exception while writing entities: ", ex);
            response.setException(ex);
        }
        return response;
    }

    @Override
    public GenericServiceAPIResponseEntity search(String query, int pageSize) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();
        try {
            EagleServiceClientImpl client = new EagleServiceClientImpl(eagleServiceHost, eagleServicePort, username, password);
            response = client.search().query(query).pageSize(pageSize).send();
            if (response.getException() != null) {
                LOG.info("Got an exception when query eagle service: " + response.getException());
            }
            client.close();
        }
        catch (Exception ex) {
            LOG.error("Got exception  when query eagle service: " + ex);
            response.setException(ex);
        }
        return response;
    }
}
