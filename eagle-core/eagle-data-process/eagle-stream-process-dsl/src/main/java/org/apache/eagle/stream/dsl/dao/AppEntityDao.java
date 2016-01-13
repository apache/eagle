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

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.storage.DataStorage;
import org.apache.eagle.storage.DataStorageManager;
import org.apache.eagle.storage.exception.IllegalDataStorageException;
import org.apache.eagle.storage.operation.Statement;
import org.apache.eagle.storage.result.ModifyResult;
import org.apache.eagle.stream.dsl.entity.AppCommandEntity;
import org.apache.eagle.stream.dsl.entity.AppDefinitionEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.QueryParam;
import java.util.List;


public interface AppEntityDao{
    GenericServiceAPIResponseEntity update(TaggedLogAPIEntity entity, String serviceName) throws Exception;
    GenericServiceAPIResponseEntity create(List<? extends TaggedLogAPIEntity> entities, String serviceName) throws Exception;
    GenericServiceAPIResponseEntity deleteByEntities(List<? extends TaggedLogAPIEntity> entities, String serviceName) throws Exception;
    GenericServiceAPIResponseEntity deleteByIds(List<String> ids, String serviceName) throws Exception;
    GenericServiceAPIResponseEntity<AppCommandEntity> search(String query, int pageSize);
}

