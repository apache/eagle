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

import org.apache.eagle.storage.jdbc.entity.JdbcEntitySerDeserHelper;
import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinition;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.torque.TorqueException;
import org.apache.torque.criteria.CriteriaInterface;
import org.apache.torque.om.mapper.RecordMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * @since 3/27/15
 */
public class AggreagteRecordMapper implements RecordMapper<Map> {
    private final static Logger LOG = LoggerFactory.getLogger(AggreagteRecordMapper.class);
    private final JdbcEntityDefinition jdbcEntityDefinition;

    public AggreagteRecordMapper(CompiledQuery query, JdbcEntityDefinition jdbcEntityDefinition) {
        this.jdbcEntityDefinition = jdbcEntityDefinition;
    }

    @Override
    public Map processRow(ResultSet resultSet, int rowOffset, CriteriaInterface<?> criteria) throws TorqueException {
        try {
            return JdbcEntitySerDeserHelper.readInternal(resultSet, this.jdbcEntityDefinition);
        } catch (SQLException e) {
            LOG.error("Failed to read result set",e);
            throw new TorqueException(e);
        } catch (IOException e) {
            LOG.error("Failed to read result set",e);
            throw new TorqueException(e);
        }
    }
}