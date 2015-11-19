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
import org.apache.eagle.storage.result.ModifyResult;

import java.io.IOException;
import java.util.List;

/**
 * @since 3/18/15
 */
public class UpdateStatement implements Statement<ModifyResult<String>> {
    private final List<? extends TaggedLogAPIEntity> entities;

    private final EntityDefinition entityDefinition;

    public UpdateStatement(List<? extends TaggedLogAPIEntity> entities,String serviceName){
        this.entities = entities;
        try {
            this.entityDefinition = EntityDefinitionManager.getEntityByServiceName(serviceName);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public UpdateStatement(List<? extends TaggedLogAPIEntity> entities,EntityDefinition entityDefinition){
        this.entities = entities;
        this.entityDefinition = entityDefinition;
    }

    @Override
    public ModifyResult<String> execute(DataStorage storage) throws IOException {
        return storage.update(entities,this.entityDefinition);
    }
}