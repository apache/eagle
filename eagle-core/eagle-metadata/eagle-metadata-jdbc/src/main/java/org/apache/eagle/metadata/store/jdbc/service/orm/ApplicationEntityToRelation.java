/*
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

package org.apache.eagle.metadata.store.jdbc.service.orm;

import org.apache.commons.lang.StringUtils;
import org.apache.eagle.common.function.ThrowableConsumer2;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.json.simple.JSONObject;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ApplicationEntityToRelation implements ThrowableConsumer2<PreparedStatement, ApplicationEntity, SQLException> {
    @Override
    public void accept(PreparedStatement statement, ApplicationEntity entity) throws SQLException {
        int parameterIndex = 1;
        if (entity.getSite() != null && StringUtils.isNotBlank(entity.getSite().getSiteId())) {
            statement.setString(parameterIndex, entity.getSite().getSiteId());
            parameterIndex++;
        }
        if (entity.getDescriptor() != null && StringUtils.isNotBlank(entity.getDescriptor().getType())) {
            statement.setString(parameterIndex, entity.getDescriptor().getType());
            parameterIndex++;
        }
        if (entity.getMode() != null && StringUtils.isNotBlank(entity.getMode().name())) {
            statement.setString(parameterIndex, entity.getMode().name());
            parameterIndex++;
        }
        if (StringUtils.isNotBlank(entity.getJarPath())) {
            statement.setString(parameterIndex, entity.getJarPath());
            parameterIndex++;
        }
        if (entity.getStatus() != null && StringUtils.isNotBlank(entity.getStatus().name())) {
            statement.setString(parameterIndex, entity.getStatus().name());
            parameterIndex++;
        }
        if (entity.getConfiguration() != null && !entity.getConfiguration().isEmpty()) {
            statement.setString(parameterIndex, JSONObject.toJSONString(entity.getConfiguration()));
            parameterIndex++;
        }
        if (entity.getContext() != null && !entity.getContext().isEmpty()) {
            statement.setString(parameterIndex, JSONObject.toJSONString(entity.getContext()));
            parameterIndex++;
        }
        if (entity.getCreatedTime() > 0) {
            statement.setLong(parameterIndex, entity.getCreatedTime());
            parameterIndex++;
        }
        if (entity.getModifiedTime() > 0) {
            statement.setLong(parameterIndex, entity.getModifiedTime());
            parameterIndex++;
        }
        if (StringUtils.isNotBlank(entity.getUuid())) {
            statement.setString(parameterIndex, entity.getUuid());
            parameterIndex++;
        }
        if (StringUtils.isNotBlank(entity.getAppId())) {
            statement.setString(parameterIndex, entity.getAppId());
            parameterIndex++;
        }
    }
}
