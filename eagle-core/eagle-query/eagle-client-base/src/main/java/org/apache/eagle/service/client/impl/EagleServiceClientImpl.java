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
package org.apache.eagle.service.client.impl;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.EagleServiceClientException;
import org.apache.eagle.service.client.EagleServiceSingleEntityQueryRequest;
import com.sun.jersey.api.client.WebResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EagleServiceClientImpl extends EagleServiceBaseClient {
    private final static Logger LOG = LoggerFactory.getLogger(EagleServiceClientImpl.class);

    public EagleServiceClientImpl(String host, int port){
        super(host, port);
    }

    public EagleServiceClientImpl(String host, int port, String username, String password){
        super(host, port, username, password);
    }

    private String getWholePath(String urlString){
    	return getBaseEndpoint() + urlString;
    }

    @Override
    public <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> create(List<E> entities, String serviceName) throws IOException,EagleServiceClientException {
        checkNotNull(serviceName,"serviceName");
        checkNotNull(entities,"entities");

        final GenericServiceAPIResponseEntity<String> response;
        response = postEntitiesWithService(GENERIC_ENTITY_PATH, entities, serviceName);
        if (!response.isSuccess()) {
            LOG.error("Failed to create entities for service: " + serviceName);
        }
        return response;
    }

    @Override
    public <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> create(List<E> entities) throws IOException, EagleServiceClientException {
        checkNotNull(entities,"entities");

        Map<String,List<E>> serviceEntityMap = groupEntitiesByService(entities);
        if(LOG.isDebugEnabled()) LOG.debug("Creating entities for "+serviceEntityMap.keySet().size()+" services");

        List<String> createdKeys = new LinkedList<String>();

        for(Map.Entry<String,List<E>> entry: serviceEntityMap.entrySet()){
            GenericServiceAPIResponseEntity<String> response = create(entry.getValue(),entry.getKey());
            if(!response.isSuccess()){
                throw new IOException("Service side exception: "+response.getException());
            }else if(response.getObj()!=null){
                createdKeys.addAll(response.getObj());
            }
        }
        GenericServiceAPIResponseEntity<String> entity = new GenericServiceAPIResponseEntity<String>();
        entity.setObj(createdKeys);
        entity.setSuccess(true);
        return entity;
    }

    @Override
    public <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> delete(List<E> entities) throws IOException, EagleServiceClientException {
        checkNotNull(entities,"entities");

        Map<String,List<E>> serviceEntityMap = groupEntitiesByService(entities);
        if(LOG.isDebugEnabled()) LOG.debug("Creating entities for "+serviceEntityMap.keySet().size()+" services");

        List<String> deletedKeys = new LinkedList<String>();
        for(Map.Entry<String,List<E>> entry: serviceEntityMap.entrySet()){
            GenericServiceAPIResponseEntity<String> response = delete(entry.getValue(), entry.getKey());
            if(!response.isSuccess()){
                LOG.error("Got service exception: "+response.getException());
                throw new IOException(response.getException());
            }else if(response.getObj()!=null){
                deletedKeys.addAll(response.getObj());
            }
        }
        GenericServiceAPIResponseEntity<String> entity = new GenericServiceAPIResponseEntity<String>();
        entity.setObj(deletedKeys);
        entity.setSuccess(true);
        return entity;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> delete(List<E> entities, String serviceName) throws IOException,EagleServiceClientException {
        checkNotNull(entities,"entities");
        checkNotNull(serviceName,"serviceName");

        return postEntitiesWithService(GENERIC_ENTITY_DELETE_PATH,entities,serviceName);
    }

    @SuppressWarnings("unchecked")
    @Override
    public GenericServiceAPIResponseEntity<String> delete(EagleServiceSingleEntityQueryRequest request) throws IOException,EagleServiceClientException {
        String queryString = request.getQueryParameterString();
        StringBuilder sb = new StringBuilder();
        sb.append(GENERIC_ENTITY_PATH);
        sb.append("?");
        sb.append(queryString);
        final String urlString =  sb.toString();

        if(!this.silence) LOG.info("Going to delete by querying service: " + getWholePath(urlString));
        WebResource r = getWebResource(urlString);
        return putAuthHeaderIfNeeded(r.accept(DEFAULT_MEDIA_TYPE)
                                      .header(CONTENT_TYPE, DEFAULT_HTTP_HEADER_CONTENT_TYPE))
                                      .delete(GenericServiceAPIResponseEntity.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public GenericServiceAPIResponseEntity<String> deleteById(List<String> ids, String serviceName) throws EagleServiceClientException, IOException {
        checkNotNull(serviceName,"serviceName");
        checkNotNull(ids,"ids");

        final String json = marshall(ids);
        final WebResource r = getWebResource(GENERIC_ENTITY_DELETE_PATH);
        return putAuthHeaderIfNeeded(r.queryParam(SERVICE_NAME,serviceName)
                                       .queryParam(DELETE_BY_ID, "true")
                                       .accept(DEFAULT_MEDIA_TYPE))
                                       .header(CONTENT_TYPE, DEFAULT_HTTP_HEADER_CONTENT_TYPE)
                                       .post(GenericServiceAPIResponseEntity.class, json);
    }

    @Override
    public <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> update(List<E> entities) throws IOException, EagleServiceClientException {
        checkNotNull(entities,"entities");

        Map<String,List<E>> serviceEntityMap = groupEntitiesByService(entities);
        if(LOG.isDebugEnabled()) LOG.debug("Updating entities for "+serviceEntityMap.keySet().size()+" services");

        List<String> createdKeys = new LinkedList<String>();

        for(Map.Entry<String,List<E>> entry: serviceEntityMap.entrySet()){
            GenericServiceAPIResponseEntity<String> response = update(entry.getValue(), entry.getKey());
            if(!response.isSuccess()){
                throw new IOException("Got service exception when updating service "+entry.getKey()+" : "+response.getException());
            }else{
                if(response.getObj()!=null) {
                    createdKeys.addAll(response.getObj());
                }
            }
        }

        GenericServiceAPIResponseEntity<String> entity = new GenericServiceAPIResponseEntity<String>();
        entity.setObj(createdKeys);
        entity.setSuccess(true);
        return entity;
    }

    @Override
    public <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> update(List<E> entities, String serviceName) throws IOException, EagleServiceClientException {
        checkNotNull(entities,"entities");
        checkNotNull(serviceName,"serviceName");

        return putEntitiesWithService(GENERIC_ENTITY_PATH,entities,serviceName);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Object> GenericServiceAPIResponseEntity<T> search(EagleServiceSingleEntityQueryRequest request) throws EagleServiceClientException {
        String queryString = request.getQueryParameterString();
        StringBuilder sb = new StringBuilder();
        sb.append(GENERIC_ENTITY_PATH);
        sb.append("?");
        sb.append(queryString);
        final String urlString =  sb.toString();
        if(!this.silence) LOG.info("Going to query service: " + getWholePath(urlString));
        WebResource r = getWebResource(urlString);
        return putAuthHeaderIfNeeded(r.accept(DEFAULT_MEDIA_TYPE))
                                       .header(CONTENT_TYPE, DEFAULT_HTTP_HEADER_CONTENT_TYPE)
                                       .get(GenericServiceAPIResponseEntity.class);
    }
}