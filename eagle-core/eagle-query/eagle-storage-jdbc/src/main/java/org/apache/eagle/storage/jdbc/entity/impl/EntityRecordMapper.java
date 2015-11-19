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
package org.apache.eagle.storage.jdbc.entity.impl;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.storage.jdbc.entity.JdbcEntitySerDeserHelper;
import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinition;
import org.apache.torque.TorqueException;
import org.apache.torque.criteria.CriteriaInterface;
import org.apache.torque.om.mapper.RecordMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

/**
 * @since 3/27/15
 */
public class EntityRecordMapper<E extends TaggedLogAPIEntity> implements RecordMapper<E> {
    private final static Logger LOG = LoggerFactory.getLogger(EntityRecordMapper.class);

    private final JdbcEntityDefinition entityDefinition;

    public EntityRecordMapper(JdbcEntityDefinition entityDefinition) {
        this.entityDefinition = entityDefinition;
    }

    @Override
    public E processRow(ResultSet resultSet, int rowOffset, CriteriaInterface<?> criteria) throws TorqueException {
        try {
            return JdbcEntitySerDeserHelper.readEntity(resultSet, entityDefinition);
        } catch (Exception e) {
            LOG.error(e.getMessage(),e);
            throw new TorqueException(e);
        }
    }
}
