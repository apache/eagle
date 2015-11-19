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
package org.apache.eagle.storage.operation;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.storage.DataStorage;
import org.apache.eagle.storage.exception.QueryCompileException;
import org.apache.eagle.storage.result.ModifyResult;

import java.io.IOException;
import java.util.List;

/**
 * @since 3/18/15
 */
public class DeleteStatement implements Statement<ModifyResult<String>> {
    private List<? extends TaggedLogAPIEntity> entities;
    private List ids = null;
    private RawQuery query;

    private EntityDefinition entityDefinition;

    public DeleteStatement(String serviceName){
        try {
            this.entityDefinition = EntityDefinitionManager.getEntityByServiceName(serviceName);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public DeleteStatement(Class<? extends TaggedLogAPIEntity> entityClass){
        try {
            this.entityDefinition = EntityDefinitionManager.getEntityDefinitionByEntityClass(entityClass);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public DeleteStatement(RawQuery query){
        this.query = query;
    }

    public void setIds(List ids){
        this.ids = ids;
    }

    public void setEntities(List<? extends TaggedLogAPIEntity> entities){
        this.entities = entities;
    }

//    public void setQuery(RawQuery query){
//        this.query = query;
//    }

    @SuppressWarnings("unchecked")
    @Override
    public ModifyResult<String> execute(DataStorage storage) throws IOException{
        ModifyResult result;
        try {
            if(this.ids !=null){
                result = storage.deleteByID(this.ids,this.entityDefinition);
            }else if(this.entities != null){
                result =storage.delete(this.entities, this.entityDefinition);
            }else if(this.query != null){
                CompiledQuery compiledQuery = storage.compile(this.query);
                this.entityDefinition = EntityDefinitionManager.getEntityByServiceName(compiledQuery.getServiceName());
                result = storage.delete(compiledQuery,this.entityDefinition);
            }else{
                throw new IllegalStateException("bad delete statement, not given enough parameters");
            }
        } catch (QueryCompileException e) {
            throw new IOException(e);
        }catch (Exception ex){
            throw new IOException(ex);
        }
        return result;
    }
}