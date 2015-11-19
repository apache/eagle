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
package org.apache.eagle.service.generic;

import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.storage.DataStorage;
import org.apache.eagle.storage.DataStorageManager;
import org.apache.eagle.storage.exception.IllegalDataStorageException;
import org.apache.eagle.storage.operation.*;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.storage.result.QueryResult;
import com.sun.jersey.api.json.JSONWithPadding;
import org.apache.commons.lang.time.StopWatch;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @since 3/18/15
 */
@Path(GenericEntityServiceResource.ROOT_PATH)
@SuppressWarnings("unchecked")
public class GenericEntityServiceResource {
    public final static String ROOT_PATH = "/entities";
    public final static String JSONP_PATH = "jsonp";
    public final static String DELETE_ENTITIES_PATH = "delete";
    public final static String ROWKEY_PATH = "rowkey";

    public final static String FIRST_TIMESTAMP = "firstTimestamp";
    public final static String LAST_TIMESTAMP = "lastTimestamp";
    public final static String ELAPSEDMS = "elapsedms";
    public final static String TOTAL_RESULTS = "totalResults";

    private final static Logger LOG = LoggerFactory.getLogger(GenericEntityServiceResource.class);

    private List<? extends TaggedLogAPIEntity> unmarshalEntitiesByServie(InputStream inputStream,EntityDefinition entityDefinition) throws IllegalAccessException, InstantiationException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(inputStream, TypeFactory.defaultInstance().constructCollectionType(LinkedList.class, entityDefinition.getEntityClass()));
    }

    private List<String> unmarshalAsStringlist(InputStream inputStream) throws IllegalAccessException, InstantiationException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(inputStream, TypeFactory.defaultInstance().constructCollectionType(LinkedList.class, String.class));
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity create(InputStream inputStream,
                                                 @QueryParam("serviceName") String serviceName){
        GenericServiceAPIResponseEntity<String> response = new GenericServiceAPIResponseEntity<String>();
        Map<String,Object> meta = new HashMap<>();
        StopWatch stopWatch = new StopWatch();
        try {
            stopWatch.start();
            EntityDefinition entityDefinition = EntityDefinitionManager.getEntityByServiceName(serviceName);

            if(entityDefinition == null){
                throw new IllegalArgumentException("entity definition of service "+serviceName+" not found");
            }

            List<? extends TaggedLogAPIEntity> entities = unmarshalEntitiesByServie(inputStream, entityDefinition);
            DataStorage dataStorage = DataStorageManager.getDataStorageByEagleConfig();
            CreateStatement createStatement = new CreateStatement(entities,entityDefinition);
            ModifyResult<String> result = createStatement.execute(dataStorage);
            if(result.isSuccess()) {
                List<String> keys =result.getIdentifiers();
                if(keys != null) {
                    response.setObj(keys, String.class);
                    response.setObj(keys, String.class);
                    meta.put(TOTAL_RESULTS,keys.size());
                }else{
                    meta.put(TOTAL_RESULTS,0);
                }
                meta.put(ELAPSEDMS,stopWatch.getTime());
                response.setMeta(meta);
                response.setSuccess(true);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            response.setException(e);
        }finally {
            stopWatch.stop();
        }
        return response;
    }

    @POST
    @Consumes({MediaType.MULTIPART_FORM_DATA})
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity create(@FormDataParam("file") InputStream fileInputStream,
                                                  @FormDataParam("file") FormDataContentDisposition cdh,
                                                  @QueryParam("serviceName") String serviceName) {
        GenericServiceAPIResponseEntity<String> response = new GenericServiceAPIResponseEntity<String>();
        Map<String,Object> meta = new HashMap<>();
        StopWatch stopWatch = new StopWatch();
        try {
            stopWatch.start();
            EntityDefinition entityDefinition = EntityDefinitionManager.getEntityByServiceName(serviceName);

            if(entityDefinition == null){
                throw new IllegalArgumentException("entity definition of service "+serviceName+" not found");
            }

            List<? extends TaggedLogAPIEntity> entities = unmarshalEntitiesByServie(fileInputStream, entityDefinition);
            DataStorage dataStorage = DataStorageManager.getDataStorageByEagleConfig();
            CreateStatement createStatement = new CreateStatement(entities,entityDefinition);
            ModifyResult<String> result = createStatement.execute(dataStorage);
            if(result.isSuccess()) {
                List<String> keys =result.getIdentifiers();
                if(keys != null) {
                    response.setObj(keys, String.class);
                    response.setObj(keys, String.class);
                    meta.put(TOTAL_RESULTS,keys.size());
                }else{
                    meta.put(TOTAL_RESULTS,0);
                }
                meta.put(ELAPSEDMS,stopWatch.getTime());
                response.setMeta(meta);
                response.setSuccess(true);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            response.setException(e);
        }finally {
            stopWatch.stop();
        }
        return response;
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity update(InputStream inputStream,
                                                 @QueryParam("serviceName") String serviceName){
        GenericServiceAPIResponseEntity<String> response = new GenericServiceAPIResponseEntity<String>();
        DataStorage dataStorage;
        Map<String,Object> meta = new HashMap<>();
        StopWatch stopWatch = new StopWatch();
        try {
            stopWatch.start();
            EntityDefinition entityDefinition = EntityDefinitionManager.getEntityByServiceName(serviceName);

            if(entityDefinition == null){
                throw new IllegalArgumentException("entity definition of service "+serviceName+" not found");
            }

            List<? extends TaggedLogAPIEntity> entities = unmarshalEntitiesByServie(inputStream, entityDefinition);
            dataStorage = DataStorageManager.getDataStorageByEagleConfig();

            UpdateStatement updateStatement = new UpdateStatement(entities,entityDefinition);
            ModifyResult<String> result = updateStatement.execute(dataStorage);
            if(result.isSuccess()) {
                List<String> keys =result.getIdentifiers();
                if(keys != null) {
                    response.setObj(keys, String.class);
                    meta.put(TOTAL_RESULTS,keys.size());
                }else{
                    meta.put(TOTAL_RESULTS,0);
                }
                meta.put(ELAPSEDMS,stopWatch.getTime());
                response.setMeta(meta);
                response.setSuccess(true);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            response.setException(e);
        } finally {
            stopWatch.stop();
        }
        return response;
    }

    @PUT
    @Consumes({MediaType.MULTIPART_FORM_DATA})
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity update(@FormDataParam("file") InputStream fileInputStream,
                                                  @FormDataParam("file") FormDataContentDisposition cdh,
                                                  @QueryParam("serviceName") String serviceName){
        GenericServiceAPIResponseEntity<String> response = new GenericServiceAPIResponseEntity<String>();
        DataStorage dataStorage;
        Map<String,Object> meta = new HashMap<>();
        StopWatch stopWatch = new StopWatch();
        try {
            stopWatch.start();
            EntityDefinition entityDefinition = EntityDefinitionManager.getEntityByServiceName(serviceName);

            if(entityDefinition == null){
                throw new IllegalArgumentException("entity definition of service "+serviceName+" not found");
            }

            List<? extends TaggedLogAPIEntity> entities = unmarshalEntitiesByServie(fileInputStream, entityDefinition);
            dataStorage = DataStorageManager.getDataStorageByEagleConfig();

            UpdateStatement updateStatement = new UpdateStatement(entities,entityDefinition);
            ModifyResult<String> result = updateStatement.execute(dataStorage);
            if(result.isSuccess()) {
                List<String> keys =result.getIdentifiers();
                if(keys != null) {
                    response.setObj(keys, String.class);
                    meta.put(TOTAL_RESULTS,keys.size());
                }else{
                    meta.put(TOTAL_RESULTS,0);
                }
                meta.put(ELAPSEDMS,stopWatch.getTime());
                response.setMeta(meta);
                response.setSuccess(true);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            response.setException(e);
        } finally {
            stopWatch.stop();
        }
        return response;
    }



    /**
     * @param value rowkey value
     * @param serviceName entity service name
     * @return GenericServiceAPIResponseEntity
     */
    @GET
    @Path(ROWKEY_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity search(@QueryParam("value") String value,@QueryParam("serviceName") String serviceName){
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();
        Map<String,Object> meta = new HashMap<>();
        DataStorage dataStorage;
        StopWatch stopWatch = null;
        try {
            if(serviceName == null) throw new IllegalArgumentException("serviceName is null");
            RowkeyQueryStatement queryStatement = new RowkeyQueryStatement(value,serviceName);
            stopWatch = new StopWatch();
            stopWatch.start();
            dataStorage = DataStorageManager.getDataStorageByEagleConfig();
            if(dataStorage==null){
                LOG.error("Data storage is null");
                throw new IllegalDataStorageException("data storage is null");
            }
            QueryResult<?> result = queryStatement.execute(dataStorage);
            if(result.isSuccess()){
                meta.put(FIRST_TIMESTAMP, result.getFirstTimestamp());
                meta.put(LAST_TIMESTAMP, result.getLastTimestamp());
                meta.put(TOTAL_RESULTS, result.getSize());
                meta.put(ELAPSEDMS,stopWatch.getTime());
                response.setObj(result.getData());
                response.setType(result.getEntityType());
                response.setSuccess(true);
                response.setMeta(meta);
                return response;
            }
        } catch (Exception e) {
            response.setException(e);
            LOG.error(e.getMessage(),e);
        }finally {
            if(stopWatch!=null) stopWatch.stop();
        }
        return response;
    }

    /**
     * @param serviceName entity service name
     * @return GenericServiceAPIResponseEntity
     */
    @POST
    @Path(ROWKEY_PATH)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity search(InputStream inputStream,@QueryParam("serviceName") String serviceName){
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();
        Map<String,Object> meta = new HashMap<>();
        DataStorage dataStorage;

        StopWatch stopWatch = null;
        try {
            if(serviceName == null) throw new IllegalArgumentException("serviceName is null");

            final List<String> values = unmarshalAsStringlist(inputStream);
            final RowkeyQueryStatement queryStatement = new RowkeyQueryStatement(values,serviceName);

            stopWatch = new StopWatch();
            stopWatch.start();
            dataStorage = DataStorageManager.getDataStorageByEagleConfig();
            if(dataStorage==null){
                LOG.error("Data storage is null");
                throw new IllegalDataStorageException("Data storage is null");
            }
            QueryResult<?> result = queryStatement.execute(dataStorage);
            if(result.isSuccess()){
                meta.put(FIRST_TIMESTAMP, result.getFirstTimestamp());
                meta.put(LAST_TIMESTAMP, result.getLastTimestamp());
                meta.put(TOTAL_RESULTS, result.getSize());
                meta.put(ELAPSEDMS,stopWatch.getTime());
                response.setObj(result.getData());
                response.setType(result.getEntityType());
                response.setSuccess(true);
                response.setMeta(meta);
                return response;
            }
        } catch (Exception e) {
            response.setException(e);
            LOG.error(e.getMessage(),e);
        }finally {
            if(stopWatch!=null) stopWatch.stop();
        }
        return response;
    }


    /**
     *
     * @param query
     * @param startTime
     * @param endTime
     * @param pageSize
     * @param startRowkey
     * @param treeAgg
     * @param timeSeries
     * @param intervalmin
     * @param top
     * @param filterIfMissing
     * @param parallel
     * @param metricName
     * @param verbose
     * @return
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @SuppressWarnings("unchecked")
    public GenericServiceAPIResponseEntity search(@QueryParam("query") String query,
                                                 @QueryParam("startTime") String startTime, @QueryParam("endTime") String endTime,
                                                 @QueryParam("pageSize") int pageSize, @QueryParam("startRowkey") String startRowkey,
                                                 @QueryParam("treeAgg") boolean treeAgg, @QueryParam("timeSeries") boolean timeSeries,
                                                 @QueryParam("intervalmin") long intervalmin, @QueryParam("top") int top,
                                                 @QueryParam("filterIfMissing") boolean filterIfMissing,
                                                 @QueryParam("parallel") int parallel,
                                                 @QueryParam("metricName") String metricName,
                                                 @QueryParam("verbose") Boolean verbose){
        RawQuery rawQuery = RawQuery.build()
                .query(query)
                .startTime(startTime)
                .endTime(endTime)
                .pageSize(pageSize)
                .startRowkey(startRowkey)
                .treeAgg(treeAgg)
                .timeSeries(timeSeries)
                .intervalMin(intervalmin)
                .top(top)
                .filerIfMissing(filterIfMissing)
                .parallel(parallel)
                .metricName(metricName)
                .verbose(verbose)
                .done();

        QueryStatement queryStatement = new QueryStatement(rawQuery);
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();
        Map<String,Object> meta = new HashMap<>();

        DataStorage dataStorage;
        StopWatch stopWatch = new StopWatch();
        try {
            stopWatch.start();
            dataStorage = DataStorageManager.getDataStorageByEagleConfig();
            if(dataStorage==null){
                LOG.error("Data storage is null");
                throw new IllegalDataStorageException("data storage is null");
            }
            
            QueryResult<?> result = queryStatement.execute(dataStorage);
            if(result.isSuccess()){
                meta.put(FIRST_TIMESTAMP, result.getFirstTimestamp());
                meta.put(LAST_TIMESTAMP, result.getLastTimestamp());
                meta.put(TOTAL_RESULTS, result.getSize());
                meta.put(ELAPSEDMS,stopWatch.getTime());

                response.setObj(result.getData());
                response.setType(result.getEntityType());
                response.setSuccess(true);
                response.setMeta(meta);
                return response;
            }
        } catch (Exception e) {
            response.setException(e);
            LOG.error(e.getMessage(),e);
        }finally {
            stopWatch.stop();
        }
        return response;
    }

    /**
     *
     * @param query
     * @param startTime
     * @param endTime
     * @param pageSize
     * @param startRowkey
     * @param treeAgg
     * @param timeSeries
     * @param intervalmin
     * @param top
     * @param filterIfMissing
     * @param parallel
     * @param metricName
     * @param verbose
     * @return
     */
    @GET
    @Path(JSONP_PATH)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public JSONWithPadding searchWithJsonp(@QueryParam("query") String query,
                                           @QueryParam("startTime") String startTime, @QueryParam("endTime") String endTime,
                                           @QueryParam("pageSize") int pageSize, @QueryParam("startRowkey") String startRowkey,
                                           @QueryParam("treeAgg") boolean treeAgg, @QueryParam("timeSeries") boolean timeSeries,
                                           @QueryParam("intervalmin") long intervalmin, @QueryParam("top") int top,
                                           @QueryParam("filterIfMissing") boolean filterIfMissing,
                                           @QueryParam("parallel") int parallel,
                                           @QueryParam("metricName") String metricName,
                                           @QueryParam("verbose") Boolean verbose,
                                           @QueryParam("callback") String callback){
        GenericServiceAPIResponseEntity result = search(query, startTime, endTime, pageSize, startRowkey, treeAgg, timeSeries, intervalmin, top, filterIfMissing, parallel, metricName, verbose);
        return new JSONWithPadding(new GenericEntity<GenericServiceAPIResponseEntity>(result){}, callback);
    }

    /**
     * TODO
     *
     * Delete by query
     *
     * @return
     */
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity deleteByQuery(@QueryParam("query") String query,
                                                 @QueryParam("startTime") String startTime, @QueryParam("endTime") String endTime,
                                                 @QueryParam("pageSize") int pageSize, @QueryParam("startRowkey") String startRowkey,
                                                 @QueryParam("treeAgg") boolean treeAgg, @QueryParam("timeSeries") boolean timeSeries,
                                                 @QueryParam("intervalmin") long intervalmin, @QueryParam("top") int top,
                                                 @QueryParam("filterIfMissing") boolean filterIfMissing,
                                                 @QueryParam("parallel") int parallel,
                                                 @QueryParam("metricName") String metricName,
                                                 @QueryParam("verbose") Boolean verbose){
        RawQuery rawQuery = RawQuery.build()
                .query(query)
                .startTime(startTime)
                .endTime(endTime)
                .pageSize(pageSize)
                .startRowkey(startRowkey)
                .treeAgg(treeAgg)
                .timeSeries(timeSeries)
                .intervalMin(intervalmin)
                .top(top)
                .filerIfMissing(filterIfMissing)
                .parallel(parallel)
                .metricName(metricName)
                .verbose(verbose)
                .done();

        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity();
        Map<String,Object> meta = new HashMap<String, Object>();
        DataStorage dataStorage = null;
        StopWatch stopWatch = new StopWatch();
        try {
            stopWatch.start();
            dataStorage = DataStorageManager.getDataStorageByEagleConfig();
            if(dataStorage==null){
                LOG.error("Data storage is null");
                throw new IllegalDataStorageException("Data storage is null");
            }
            
            DeleteStatement deleteStatement = new DeleteStatement(rawQuery);
            ModifyResult<String> deleteResult = deleteStatement.execute(dataStorage);
            if(deleteResult.isSuccess()){
                meta.put(ELAPSEDMS, stopWatch.getTime());
                response.setObj(deleteResult.getIdentifiers(),String.class);
                response.setSuccess(true);
                response.setMeta(meta);
            }
            return response;
        } catch (Exception e) {
            response.setException(e);
            LOG.error(e.getMessage(),e);
        }finally {
            stopWatch.stop();
        }
        return response;
    }

    /**
     *
     * Delete by entity lists
     *
     * Use "POST /entities/delete" instead of "DELETE  /entities" to walk around jersey DELETE issue for request with body
     *
     * @param inputStream
     * @param serviceName
     * @return
     */
    @POST
    @Path(DELETE_ENTITIES_PATH)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public GenericServiceAPIResponseEntity deleteEntities(InputStream inputStream,
                                                 @QueryParam("serviceName") String serviceName,
                                                 @QueryParam("byId") Boolean deleteById){
        GenericServiceAPIResponseEntity<String> response = new GenericServiceAPIResponseEntity<String>();
        DataStorage dataStorage = null;
        Map<String,Object> meta = new HashMap<String, Object>();

        if(deleteById == null) deleteById = false;

        StopWatch stopWatch = new StopWatch();

        try {
            stopWatch.start();
            dataStorage = DataStorageManager.getDataStorageByEagleConfig();
            DeleteStatement statement = new DeleteStatement(serviceName);

            if(deleteById) {
                LOG.info("Deleting "+serviceName+" by ids");
                List<String> deleteIds = unmarshalAsStringlist(inputStream);
                statement.setIds(deleteIds);
            }else {
                LOG.info("Deleting "+serviceName+" by entities");
                EntityDefinition entityDefinition = EntityDefinitionManager.getEntityByServiceName(serviceName);
                if (entityDefinition == null) {
                    throw new IllegalArgumentException("Entity definition of service " + serviceName + " not found");
                }
                List<? extends TaggedLogAPIEntity> entities = unmarshalEntitiesByServie(inputStream, entityDefinition);
                statement.setEntities(entities);
            }

            ModifyResult<String> result = statement.execute(dataStorage);
            if (result.isSuccess()) {
                List<String> keys = result.getIdentifiers();
                if (keys != null) {
                    response.setObj(keys, String.class);
                    meta.put(TOTAL_RESULTS, keys.size());
                } else {
                    meta.put(TOTAL_RESULTS, 0);
                }
                meta.put(ELAPSEDMS, stopWatch.getTime());
                response.setMeta(meta);
                response.setSuccess(true);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            response.setException(e);
        }finally {
            stopWatch.stop();
        }
        return response;
    }
}