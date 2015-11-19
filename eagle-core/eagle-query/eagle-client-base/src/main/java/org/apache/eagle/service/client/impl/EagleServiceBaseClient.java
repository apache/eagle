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

import com.sun.jersey.api.client.AsyncWebResource;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.eagle.common.Base64;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.service.client.EagleServiceAsyncClient;
import org.apache.eagle.service.client.EagleServiceClientException;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.security.SecurityConstants;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public abstract class EagleServiceBaseClient implements IEagleServiceClient {
    public final static String SERVICE_NAME="serviceName";
    public final static String DELETE_BY_ID="byId";

    private final String host;
    private final int port;
    private final String basePath;
    private String username;
    private String password;
    protected boolean silence = false;

    public String getBaseEndpoint() {
        return baseEndpoint;
    }

    private final String baseEndpoint;

    private final static Logger LOG = LoggerFactory.getLogger(EagleServiceBaseClient.class);

    protected static final String DEFAULT_BASE_PATH = "/eagle-service/rest";
    protected static final MediaType DEFAULT_MEDIA_TYPE = MediaType.APPLICATION_JSON_TYPE;
    protected static final String DEFAULT_HTTP_HEADER_CONTENT_TYPE = "application/json";
    protected static final String CONTENT_TYPE = "Content-Type";

    protected final static String GENERIC_ENTITY_PATH = "/entities";
    protected final static String GENERIC_ENTITY_DELETE_PATH = GENERIC_ENTITY_PATH+"/delete";
    private final Client client;
    private final List<Closeable> closeables = new LinkedList<Closeable>();

    private volatile boolean isStopped = false;

    public EagleServiceBaseClient(String host, int port, String basePath, String username, String password) {
        this.host = host;
        this.port = port;
        this.basePath = basePath;
        this.baseEndpoint = buildBathPath().toString();
        this.username = username;
        this.password = password;

        ClientConfig cc = new DefaultClientConfig();
        cc.getProperties().put(DefaultClientConfig.PROPERTY_CONNECT_TIMEOUT, 60 * 1000);
        cc.getProperties().put(DefaultClientConfig.PROPERTY_READ_TIMEOUT, 60 * 1000);
        cc.getClasses().add(JacksonJsonProvider.class);
        cc.getProperties().put(URLConnectionClientHandler.PROPERTY_HTTP_URL_CONNECTION_SET_METHOD_WORKAROUND, true);
        this.client = Client.create(cc);
        client.addFilter(new com.sun.jersey.api.client.filter.GZIPContentEncodingFilter());
        //        Runtime.getRuntime().addShutdownHook(new EagleServiceClientShutdownHook(this));
    }

    public EagleServiceBaseClient(String host, int port, String basePath){
        this(host, port, basePath, null);
    }

    public EagleServiceBaseClient(String host, int port, String username, String password){
        this(host, port, DEFAULT_BASE_PATH, username, password);
    }

//    private class EagleServiceClientShutdownHook extends Thread{
//        final IEagleServiceClient client;
//        EagleServiceClientShutdownHook(IEagleServiceClient client){
//            this.client = client;
//        }
//
//        @Override
//        public void run() {
//            LOG.info("Client shutdown hook");
//            try {
//                this.client.close();
//            } catch (IOException e) {
//                LOG.error(e.getMessage(),e);
//            }
//        }
//    }

    public Client getJerseyClient(){
        return client;
    }

    public EagleServiceBaseClient(String host, int port){
        this(host,port,DEFAULT_BASE_PATH);
    }

    protected final StringBuilder buildBathPath() {
        StringBuilder sb = new StringBuilder();
        sb.append("http://");
        sb.append(host);
        sb.append(":");
        sb.append(port);
        sb.append(basePath);
        return sb;
    }

    protected static String marshall(List<?> entities) throws JsonMappingException, JsonGenerationException, IOException {
        final JsonFactory factory = new JsonFactory();
        final ObjectMapper mapper = new ObjectMapper(factory);
        mapper.setFilters(TaggedLogAPIEntity.getFilterProvider());
        return mapper.writeValueAsString(entities);
    }

    protected <E extends TaggedLogAPIEntity> Map<String,List<E>> groupEntitiesByService(List<E> entities) throws EagleServiceClientException {
        Map<String,List<E>> serviceEntityMap = new HashMap<String, List<E>>();
        if(LOG.isDebugEnabled()) LOG.debug("Grouping entities by service name");
        for(E entity: entities){
            if(entity == null) {
                LOG.warn("Skip null entity");
                continue;
            }

            try {
                EntityDefinition entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(entity.getClass());
                if(entityDefinition == null){
                    throw new EagleServiceClientException("Failed to find entity definition of class: "+entity.getClass());
                }
                String serviceName = entityDefinition.getService();
                List<E> bucket = serviceEntityMap.get(serviceName);
                if(bucket == null){
                    bucket = new LinkedList<E>();
                    serviceEntityMap.put(serviceName, bucket);
                }
                bucket.add(entity);
            } catch (InstantiationException e) {
                throw new EagleServiceClientException(e);
            } catch (IllegalAccessException e) {
                throw new EagleServiceClientException(e);
            }
        }
        return serviceEntityMap;
    }

    @Override
    public SearchRequestBuilder search() {
        return new SearchRequestBuilder(this);
    }

    @Override
    public SearchRequestBuilder search(String query) {
        return new SearchRequestBuilder(this).query(query);
    }

    protected void register(Closeable closeable){
        this.closeables.add(closeable);
    }

    @Override
    public MetricSender metric(String metricName) {
        MetricSender metricGenerator = new MetricSender(this,metricName);
        this.register(metricGenerator);
        return metricGenerator;
    }

    protected WebResource getWebResource(String relativePath){
        return this.getJerseyClient().resource(this.getBaseEndpoint() + relativePath);
    }

    protected AsyncWebResource getAsyncWebResource(String relativePath){
        return this.getJerseyClient().asyncResource(this.getBaseEndpoint() + relativePath);
    }

    protected WebResource.Builder putAuthHeaderIfNeeded(WebResource.Builder r) {
        if (username != null && password != null) {
           r.header(SecurityConstants.AUTHORIZATION, SecurityConstants.BASIC_AUTHORIZATION_HEADER_PREFIX + Base64.encode(username + ":" + password));
        }
        return r;
    }

    /**
     * Send HTTP POST request with entities and serviceName
     *
     * @param resourceURL
     * @param entities
     * @param serviceName
     * @return
     * @throws JsonMappingException
     * @throws JsonGenerationException
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    protected GenericServiceAPIResponseEntity<String> postEntitiesWithService(String resourceURL, List<? extends TaggedLogAPIEntity> entities,String serviceName) throws JsonMappingException, JsonGenerationException, IOException {
        final String json = marshall(entities);
        final WebResource r = getWebResource(resourceURL);
        return putAuthHeaderIfNeeded(r.queryParam(SERVICE_NAME,serviceName).accept(DEFAULT_MEDIA_TYPE))
                .header(CONTENT_TYPE, DEFAULT_HTTP_HEADER_CONTENT_TYPE)
                .post(GenericServiceAPIResponseEntity.class, json);
    }

    /**
     * Send HTTP PUT request with entities and serviceName
     *
     * @param resourceURL
     * @param entities
     * @param serviceName
     * @return
     * @throws JsonMappingException
     * @throws JsonGenerationException
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    protected GenericServiceAPIResponseEntity<String> putEntitiesWithService(String resourceURL, List<? extends TaggedLogAPIEntity> entities,String serviceName) throws JsonMappingException, JsonGenerationException, IOException {
        final String json = marshall(entities);
        final WebResource r = getWebResource(resourceURL);
        return putAuthHeaderIfNeeded(r.queryParam(SERVICE_NAME,serviceName).accept(DEFAULT_MEDIA_TYPE))
                .header(CONTENT_TYPE, DEFAULT_HTTP_HEADER_CONTENT_TYPE)
                .put(GenericServiceAPIResponseEntity.class, json);
    }


    protected <E extends TaggedLogAPIEntity> String getServiceNameByClass(Class<E> entityClass) throws EagleServiceClientException {
        EntityDefinition entityDefinition = null;
        try {
            entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(entityClass);
        } catch (InstantiationException e) {
            throw new EagleServiceClientException(e);
        } catch (IllegalAccessException e) {
            throw new EagleServiceClientException(e);
        }

        if(entityDefinition == null){
            throw new EagleServiceClientException("cannot find entity definition of class "+entityClass);
        }
        return entityDefinition.getService();
    }


    @Override
    public BatchSender batch(int batchSize) {
        BatchSender batchSender = new BatchSender(this,batchSize);
        this.register(batchSender);
        return batchSender;
    }

    @Override
    public EagleServiceAsyncClient async() {
        EagleServiceAsyncClient async = new EagleServiceAsyncClientImpl(this);
        this.register(async);
        return async;
    }

    @Override
    public ConcurrentSender parallel(int parallelNum) {
        ConcurrentSender concurrentSender = new ConcurrentSender(this,parallelNum);
        this.register(concurrentSender);
        return concurrentSender;
    }

    @Override
    public <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> delete(List<E> entities, Class<E> entityClass) throws IOException, EagleServiceClientException {
        return delete(entities, getServiceNameByClass(entityClass));
    }

    @Override
    public <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> create(List<E> entities, Class<E> entityClass) throws IOException, EagleServiceClientException {
        return create(entities, getServiceNameByClass(entityClass));
    }

    @Override
    public <E extends TaggedLogAPIEntity> GenericServiceAPIResponseEntity<String> update(List<E> entities, Class<E> entityClass) throws IOException, EagleServiceClientException {
        return update(entities, getServiceNameByClass(entityClass));
    }

    @Override
    public void close() throws IOException {
        if(!this.isStopped) {
            if(LOG.isDebugEnabled()) LOG.debug("Client is closing");
            for (Closeable closeable : this.closeables) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                    throw e;
                }
            }
        }
        this.isStopped = true;
    }

    @Override
    public DeleteRequestBuilder delete() {
        return new DeleteRequestBuilder(this);
    }

    protected void checkNotNull(Object obj,String name) throws EagleServiceClientException{
        if(obj == null) throw new EagleServiceClientException(name+" should not be null but given");
    }

    @Override
    public EagleServiceBaseClient silence(boolean silence) {
        this.silence = silence;
        return this;
    }
}