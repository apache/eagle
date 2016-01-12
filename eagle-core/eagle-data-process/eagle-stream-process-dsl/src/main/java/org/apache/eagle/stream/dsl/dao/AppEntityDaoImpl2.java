/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.stream.dsl.dao;

import org.apache.commons.lang.time.StopWatch;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.generic.GenericEntityServiceResource;
import org.apache.eagle.storage.DataStorage;
import org.apache.eagle.storage.DataStorageManager;
import org.apache.eagle.storage.exception.IllegalDataStorageException;
import org.apache.eagle.storage.operation.CreateStatement;
import org.apache.eagle.storage.operation.DeleteStatement;
import org.apache.eagle.storage.operation.Statement;
import org.apache.eagle.storage.operation.UpdateStatement;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.stream.dsl.entity.AppCommandEntity;
import org.apache.eagle.stream.dsl.rest.AppConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class AppEntityDaoImpl2 implements AppEntityDao {
    private final static Logger LOG = LoggerFactory.getLogger(AppEntityDaoImpl2.class);

    public GenericServiceAPIResponseEntity updateEntities(Statement<ModifyResult<String>> statement) throws Exception {
        GenericServiceAPIResponseEntity<String> response = new GenericServiceAPIResponseEntity<>();
        Map<String,Object> meta = new HashMap<>();
        StopWatch stopWatch = new StopWatch();

        try {
            stopWatch.start();
            DataStorage dataStorage = DataStorageManager.getDataStorageByEagleConfig();
            if(dataStorage == null){
                LOG.error("Data storage is null");
                throw new IllegalDataStorageException("Data storage is null");
            }
            ModifyResult<String> result = statement.execute(dataStorage);
            if(result.isSuccess()) {
                List<String> keys =result.getIdentifiers();
                if(keys != null) {
                    response.setObj(keys, String.class);
                    meta.put(AppConstants.TOTAL_RESULTS, keys.size());
                } else {
                    meta.put(AppConstants.TOTAL_RESULTS, 0);
                }
                meta.put(AppConstants.ELAPSEDMS,stopWatch.getTime());
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


    public GenericServiceAPIResponseEntity update(List<? extends TaggedLogAPIEntity> entities, String serviceName) throws Exception {
        UpdateStatement statement = new UpdateStatement(entities, serviceName);
        GenericServiceAPIResponseEntity<String> response = updateEntities(statement);
        return response;
    }


    public GenericServiceAPIResponseEntity create(List<? extends TaggedLogAPIEntity> entities, String serviceName) throws Exception{
        CreateStatement statement = new CreateStatement(entities, serviceName);
        GenericServiceAPIResponseEntity<String> response = updateEntities(statement);
        return response;
    }

    @Override
    public GenericServiceAPIResponseEntity deleteByEntities(List<? extends TaggedLogAPIEntity> entities, String serviceName) throws Exception {
        DeleteStatement statement = new DeleteStatement(serviceName);
        statement.setEntities(entities);
        GenericServiceAPIResponseEntity<String> response = updateEntities(statement);
        return response;
    }

    @Override
    public GenericServiceAPIResponseEntity deleteByIds(List<String> ids, String serviceName) throws Exception {
        DeleteStatement statement = new DeleteStatement(serviceName);
        statement.setIds(ids);
        GenericServiceAPIResponseEntity<String> response = updateEntities(statement);
        return response;
    }

    @Override
    public GenericServiceAPIResponseEntity<AppCommandEntity> search(String query, int pageSize) {
        GenericServiceAPIResponseEntity response = new GenericServiceAPIResponseEntity<>();
        try {
            GenericEntityServiceResource entityServiceResource = new GenericEntityServiceResource();
            response = entityServiceResource.search(query, null, null, pageSize, null, false, false, 0, Integer.MAX_VALUE, true, 0, null, false);
        } catch (Exception ex) {
            response.setException(ex);
        }
        return response;
    }


}